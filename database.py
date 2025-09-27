from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent  # папка где лежит database.py
DB_PATH = (BASE_DIR.parent / "data" / "emias_bot.db")  # поднялись на уровень выше и в data/

DB_PATH.parent.mkdir(parents=True, exist_ok=True)  # на всякий случай создадим папку

DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()
class UserToken(Base):
    __tablename__ = 'user_tokens'
    id = Column(Integer, primary_key=True, index=True)
    telegram_user_id = Column(Integer, unique=True, index=True)
    access_token = Column(String)
    refresh_token = Column(String)
    expires_at = Column(DateTime)


from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Промежуточная таблица для связи UserProfile и Specialty (многие к многим)
user_specialties = Table(
    'user_specialties',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('user_profiles.id'), primary_key=True),
    Column('specialty_id', Integer, ForeignKey('specialties.id'), primary_key=True)
)

class UserProfile(Base):
    __tablename__ = 'user_profiles'
    id = Column(Integer, primary_key=True, index=True)
    telegram_user_id = Column(Integer, unique=True, index=True)
    oms_number = Column(String)
    birth_date = Column(String)
    is_admin = Column(Boolean, default=False)
    password = Column(String, nullable=True)  # Пароль для веб-доступа
    # Связь с специальностями
    specialties = relationship("Specialty", secondary=user_specialties, back_populates="users")

class Specialty(Base):
    __tablename__ = 'specialties'
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True)   # Например, "599621" или "2028"
    name = Column(String)
    # app_id (удалено из использования): ранее пытались хранить app_id глобально, теперь используем только appointment_id на связке
    # Оставляем колонку в БД (если уже существует) для совместимости, но не объявляем заново, чтобы новые инстансы не трогали её.
    # (Физический дроп в SQLite без миграции не делаем.)

    ar_inquiry_purpose_code = Column(Integer, nullable=True, default=264)
    ar_inquiry_purpose_id = Column(Integer, nullable=True, default=76)

    appointment_duration = Column(Integer, nullable=True)  # в минутах, например
    reception_type_id = Column(Integer, nullable=True)
    # Новая унифицированная политика направления: 0=strict,1=fallback,2=always_allow
    referral_policy = Column(Integer, default=1)
    # requires_referral устарел и удалён через миграцию


    # Обратная связь к пользователям, подписанным на данную специальность
    users = relationship("UserProfile", secondary=user_specialties, back_populates="specialties")

class DoctorInfo(Base):
    __tablename__ = 'doctor_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    doctor_api_id = Column(String, nullable=False, unique=True)  # API-идентификатор врача
    name = Column(String, nullable=False)
    complex_resource_id = Column(String, nullable=True)           # ID complexResource, берем первый элемент
    ar_speciality_id = Column(String, nullable=True)                # arSpecialityId (например, "2028")
    ar_speciality_name = Column(String, nullable=True)              # arSpecialityName (например, "Заболевание кожи...")

    def __repr__(self):
        return (f"<DoctorInfo(doctor_api_id={self.doctor_api_id}, name={self.name}, "
                f"complex_resource_id={self.complex_resource_id}, "
                f"ar_speciality_id={self.ar_speciality_id}, "
                f"ar_speciality_name={self.ar_speciality_name}, "
                f"ar_inquiry_purpose_code={self.ar_inquiry_purpose_code}, "
                f"ar_inquiry_purpose_id={self.ar_inquiry_purpose_id})>")


class UserFavoriteDoctor(Base):
    __tablename__ = 'user_favorite_doctors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True)
    doctor_api_id = Column(String, ForeignKey('doctor_info.doctor_api_id'), nullable=False)
    # app_id больше не используется – оставлено только для старых данных

    # Связь с DoctorInfo
    doctor = relationship("DoctorInfo")

    # Гарантируем, что один пользователь не добавит одного и того же врача дважды
    __table_args__ = (
        UniqueConstraint('telegram_user_id', 'doctor_api_id', name='uq_user_doctor'),
    )

    def __repr__(self):
        return f"<UserFavoriteDoctor(telegram_user_id={self.telegram_user_id}, doctor_api_id={self.doctor_api_id})>"
from sqlalchemy import Column, Integer, String, Boolean, JSON
class UserTrackedDoctor(Base):
    __tablename__ = 'user_tracked_doctors'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True)
    doctor_api_id = Column(String, nullable=False)
    # app_id больше не используется
    auto_booking = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    tracking_rules = Column(JSON, nullable=True)  # Массив правил

    # Уникальность: один пользователь может отслеживать врача только один раз
    __table_args__ = (
        UniqueConstraint('telegram_user_id', 'doctor_api_id', name='uq_user_tracked_doctor'),
    )

    def __repr__(self):
        return f"<UserTrackedDoctor(telegram_user_id={self.telegram_user_id}, doctor_api_id={self.doctor_api_id})>"

from sqlalchemy import Column, Integer, String, ForeignKey, Text
from sqlalchemy.orm import relationship
from database import Base


class DoctorSchedule(Base):
    """
    Таблица для хранения последнего известного расписания врача.
    """
    __tablename__ = "doctor_schedules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doctor_api_id = Column(String, ForeignKey("doctor_info.doctor_api_id"), unique=True, nullable=False)
    schedule_text = Column(Text, nullable=False)  # Текст расписания (JSON list of days)
    updated_at = Column(DateTime, default=datetime.utcnow)  # Когда обновлено

    # Связь с таблицей DoctorInfo
    doctor = relationship("DoctorInfo", backref="schedule")

    def __repr__(self):
        return f"<DoctorSchedule(doctor_api_id={self.doctor_api_id}, schedule_text={self.schedule_text[:30]}...)>"


class UserDoctorLink(Base):
    __tablename__ = 'user_doctor_link'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True)
    doctor_speciality = Column(String, nullable=False)
    appointment_id = Column(String, nullable=True)
    # Возвращаем referral_id: теперь снова нужно хранить связанное направление (по запросу пользователя)
    referral_id = Column(String, nullable=True)
    # app_id больше не используется; используем appointment_id

    __table_args__ = (
        UniqueConstraint('telegram_user_id', 'doctor_speciality', name='uq_user_doctor_appointment'),
    )

class UserLog(Base):
    __tablename__ = 'user_logs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(Integer, index=True)
    action = Column(String, nullable=False)  # e.g., 'login', 'add_track', 'delete_favorite'
    timestamp = Column(DateTime, default=datetime.utcnow)
    details = Column(String, nullable=True)  # JSON or text details
    source = Column(String, nullable=True)   # 'web' | 'bot' | 'system'
    status = Column(String, nullable=True)   # 'success' | 'error' | 'info' | 'warning'

    def __repr__(self):
        return f"<UserDoctorLink(telegram_user_id={self.telegram_user_id}, doctor_speciality={self.doctor_speciality}, appointment_id={self.appointment_id})>"


def is_tracking_doctor(session, user_id: int, doctor_api_id: str) -> bool:
    """ Проверяет, отслеживает ли пользователь данного врача. """
    return session.query(UserTrackedDoctor).filter_by(
        telegram_user_id=user_id, doctor_api_id=doctor_api_id
    ).first() is not None

def add_tracking_doctor(session, user_id: int, doctor_api_id: str):
    """ Добавляет врача в список отслеживаемых. """
    tracking = UserTrackedDoctor(telegram_user_id=user_id, doctor_api_id=doctor_api_id)
    session.add(tracking)

def init_db():
    Base.metadata.create_all(bind=engine)


def get_db_session():
    return SessionLocal()

def add_favorite_doctor(session, telegram_user_id: int, doctor_api_id: str):
    """
    Добавляет врача в избранное для пользователя.
    Если такой врач уже добавлен, функция ничего не делает.
    """
    favorite = session.query(UserFavoriteDoctor).filter_by(
        telegram_user_id=telegram_user_id, doctor_api_id=doctor_api_id
    ).first()
    if favorite:
        print(f"Doctor {doctor_api_id} уже в избранном для пользователя {telegram_user_id}.")
        return favorite

    favorite = UserFavoriteDoctor(
        telegram_user_id=telegram_user_id,
        doctor_api_id=doctor_api_id
    )
    session.add(favorite)
    session.commit()
    print(f"Добавлен врач {doctor_api_id} в избранное для пользователя {telegram_user_id}.")
    return favorite

def remove_favorite_doctor(session, telegram_user_id: int, doctor_api_id: str):
    """
    Удаляет врача из избранного для пользователя.
    Если врач не найден, ничего не делает.
    """
    favorite = session.query(UserFavoriteDoctor).filter_by(
        telegram_user_id=telegram_user_id,
        doctor_api_id=doctor_api_id
    ).first()
    if favorite:
        session.delete(favorite)
        session.commit()
        print(f"Удален врач {doctor_api_id} из избранного для пользователя {telegram_user_id}.")
    else:
        print(f"Врач {doctor_api_id} не найден в избранном у пользователя {telegram_user_id}.")


def list_favorite_doctors(session, telegram_user_id: int):
    """
    Возвращает список всех избранных врачей для пользователя.
    """
    return session.query(UserFavoriteDoctor).filter_by(telegram_user_id=telegram_user_id).all()


def list_tracked_doctors(session, telegram_user_id: int):
    """
    Возвращает список отслеживаемых врачей (объекты модели UserTrackedDoctor) для пользователя.
    """
    tracked = session.query(UserTrackedDoctor).filter_by(telegram_user_id=telegram_user_id).all()
    return tracked


def init_db():
    Base.metadata.create_all(bind=engine)

def get_db_session():
    return SessionLocal()

def save_tokens(session, telegram_user_id: int, access_token: str, refresh_token: str, expires_in: int):
    from datetime import datetime
    import time
    expires_at = datetime.fromtimestamp(time.time() + expires_in)
    user_token = session.query(UserToken).filter_by(telegram_user_id=telegram_user_id).first()
    if not user_token:
        user_token = UserToken(
            telegram_user_id=telegram_user_id,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at
        )
        session.add(user_token)
    else:
        user_token.access_token = access_token
        user_token.refresh_token = refresh_token
        user_token.expires_at = expires_at
    session.commit()

def get_tokens(session, telegram_user_id: int):
    user_token = session.query(UserToken).filter_by(telegram_user_id=telegram_user_id).first()
    # print(user_token)
    if user_token:
        return (user_token.access_token, user_token.refresh_token, user_token.expires_at)
    return None

def save_profile(session, telegram_user_id: int, oms_number: str, birth_date: str):
    profile = session.query(UserProfile).filter_by(telegram_user_id=telegram_user_id).first()
    if not profile:
        profile = UserProfile(
            telegram_user_id=telegram_user_id,
            oms_number=oms_number,
            birth_date=birth_date
        )
        session.add(profile)
    else:
        profile.oms_number = oms_number
        profile.birth_date = birth_date
    session.commit()

def get_profile(session, telegram_user_id: int):
    return session.query(UserProfile).filter_by(telegram_user_id=telegram_user_id).first()

import json

# --- Специализации-синонимы ---
# Пользователь запросил эквивалентность только для ВОП / терапевт.
# Если нужно ограничить: оставляем только реальную группу (например: 69,209,2) и исключаем 602.
SPECIALITY_ALIASES = {
    # ВОП / терапевт: оставляем взаимную эквивалентность (если 602 сюда действительно относится)
    "69": {"69", "602"},
    "602": {"69", "602"},
}

def get_equivalent_speciality_codes(code):
    """
    Возвращает множество эквивалентных кодов специальности.
    Например: 69 <-> 602. Для остальных возвращает сам код.
    """
    code_str = str(code) if code is not None else ""
    return SPECIALITY_ALIASES.get(code_str, {code_str} if code_str else set())

def save_or_update_doctor(session, telegram_user_id: int, doctor_data: dict):
    """
    Сохраняет или обновляет запись о враче в базе данных, а также связь с пользователем.

    doctor_data: словарь с данными врача, например:
        {
            "id": 20828145710,
            "name": "Зверев А. Д. <16>",
            "arSpecialityId": 2028,
            "arSpecialityName": "Заболевание кожи (исключая новообразования кожи)",
            "complexResource": [{"id": 607187938, "name": "81"}],
            "appointment_id": "some_appointment_id"  # ID для записи
            ...  # Другие поля
        }

    Если arSpecialityId и arSpecialityName отсутствуют, но есть ldpType, используем первый элемент ldpType:
        ar_speciality_id = ldpType[0]["code"]
        ar_speciality_name = ldpType[0]["name"]

    Сохраняются:
      - doctor_api_id: значение doctor_data["id"]
      - name: doctor_data["name"]
      - complex_resource_id: значение doctor_data["complexResource"][0]["id"] (если есть)
      - ar_speciality_id: значение doctor_data["arSpecialityId"] или ldpType[0]["code"]
      - ar_speciality_name: значение doctor_data["arSpecialityName"] или ldpType[0]["name"]
      - appointment_id: в таблице UserDoctorLink
    Дополнительно: если speciality (включая ldpType) ещё не существует в таблице specialties,
    автоматически создаём её с referral_policy по умолчанию.
    """
    doctor_api_id = str(doctor_data.get("id"))
    name = doctor_data.get("name")

    # Обрабатываем complexResource – берем первый элемент, если он есть
    complex_resource_list = doctor_data.get("complexResource", [])
    complex_resource_id = None
    if complex_resource_list and isinstance(complex_resource_list, list):
        complex_resource_id = str(complex_resource_list[0].get("id"))

    # --- Унифицированное извлечение speciality (для RECEPTION и LDP) ---
    # Приоритет: arSpecialityId / arSpecialityName, иначе первый элемент ldpType.
    ar_speciality_id = None
    ar_speciality_name = None
    if doctor_data.get("arSpecialityId") is not None:
        ar_speciality_id = str(doctor_data.get("arSpecialityId"))
        ar_speciality_name = doctor_data.get("arSpecialityName")
    else:
        ldp_types = doctor_data.get("ldpType", [])
        if ldp_types:
            first_ldp = ldp_types[0] or {}
            ar_speciality_id = str(first_ldp.get("code")) if first_ldp.get("code") is not None else None
            ar_speciality_name = first_ldp.get("name")

    # Если получили код специальности / ldpType – гарантируем наличие строки в Specialty.
    # Это позволит применять referral_policy и другие настройки и к ldpType.
    if ar_speciality_id:
        try:
            spec_obj = session.query(Specialty).filter_by(code=ar_speciality_id).first()
            if not spec_obj:
                # Определяем является ли эта специальность LDP по наличию блока ldpType.
                is_ldp = bool(doctor_data.get("ldpType"))
                # Для новых LDP по требованию: referral_policy = 0 (строгий режим, требуется направление)
                rp = 0 if is_ldp else 1
                spec_obj = Specialty(
                    code=ar_speciality_id,
                    name=ar_speciality_name or ar_speciality_id,
                    referral_policy=rp
                )
                session.add(spec_obj)
                try:
                    log_user_action(
                        session,
                        telegram_user_id,
                        'specialty_autocreate',
                        details=f'code={ar_speciality_id} name={ar_speciality_name} is_ldp={is_ldp} rp={rp}',
                        source='system',
                        status='info'
                    )
                except Exception:
                    pass
        except Exception:
            # Не критично для сохранения врача – просто пропускаем.
            pass

    # Ищем, существует ли уже запись с данным API-идентификатором
    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=doctor_api_id).first()
    if doctor:
        # Обновляем запись с защитой от перезаписи информативного имени (например, 'СМАД 321')
        def _should_update_name(old: str | None, new: str | None) -> bool:
            if not new:
                return False
            if not old:
                return True
            import re
            old_up = old.upper()
            new_up = new.upper()
            # Если старое имя содержит 'СМАД' или номер кабинета, а новое его не содержит — не трогаем
            has_old_cab_digits = bool(re.search(r"\d+", old))
            has_new_cab_digits = bool(re.search(r"\d+", new))
            if ('СМАД' in old_up or 'СУТОЧ' in old_up) and not ('СМАД' in new_up or 'СУТОЧ' in new_up):
                return False
            if has_old_cab_digits and not has_new_cab_digits:
                return False
            # Если новое имя гораздо короче и выглядит как общее описание услуги, оставим старое
            if len(new) < 8 and len(old) >= 8:
                return False
            return True
        if _should_update_name(doctor.name, name):
            doctor.name = name
        if complex_resource_id is not None:
            doctor.complex_resource_id = complex_resource_id
        doctor.ar_speciality_id = ar_speciality_id
        doctor.ar_speciality_name = ar_speciality_name
    else:
        # Создаем новую запись
        doctor = DoctorInfo(
            doctor_api_id=doctor_api_id,
            name=name,
            complex_resource_id=complex_resource_id,
            ar_speciality_id=ar_speciality_id,
            ar_speciality_name=ar_speciality_name
        )
        session.add(doctor)
        # print(f"Added doctor {doctor_api_id}")

    # Обновляем или создаем связь "пользователь-врач" с appointment_id
    appointment_id = doctor_data.get("appointment_id")
    if appointment_id:
        # Обновляем/создаём ссылки для всех эквивалентных кодов специальности (69 <-> 209)
        for spec_code in get_equivalent_speciality_codes(ar_speciality_id):
            link = session.query(UserDoctorLink).filter_by(
                telegram_user_id=telegram_user_id,
                doctor_speciality=spec_code
            ).first()
            if link:
                link.appointment_id = str(appointment_id)
            else:
                link = UserDoctorLink(
                    telegram_user_id=telegram_user_id,
                    doctor_speciality=spec_code,
                    appointment_id=str(appointment_id)
                )
                session.add(link)

        # Также синхронизируем app_id в отслеживании (если уже отслеживается этот врач)
        tracking = session.query(UserTrackedDoctor).filter_by(
            telegram_user_id=telegram_user_id,
            doctor_api_id=doctor_api_id
        ).first()
        # tracking.app_id удалён из использования

    return doctor

def save_or_update_doctors(session, telegram_user_id: int, doctors_data: list):
    """
    Обрабатывает список врачей: для каждого врача вызывает save_or_update_doctor,
    затем делает один commit в конце транзакции.

    :param doctors_data: список словарей с данными врачей
    """
    for doctor_data in doctors_data:
        save_or_update_doctor(session, telegram_user_id, doctor_data)
    session.commit()
    print("All doctors processed and committed.")

def log_user_action(session, telegram_user_id: int, action: str, details: str = None, *, source: str = 'unknown', status: str = 'info'):
    """Логирует действие пользователя c указанием источника и статуса.

    :param source: 'web' | 'bot' | 'system' | 'unknown'
    :param status: 'success' | 'error' | 'info' | 'warning'
    """
    # Подавление частых одинаковых ошибок обновления токена
    if action == 'api_refresh_token' and status == 'error':
        try:
            from datetime import datetime, timedelta
            recent = (
                session.query(UserLog)
                .filter(UserLog.telegram_user_id == telegram_user_id,
                        UserLog.action == action,
                        UserLog.status == 'error')
                .order_by(UserLog.timestamp.desc())
                .first()
            )
            if recent and recent.details == details and recent.timestamp >= datetime.utcnow() - timedelta(minutes=60):
                return  # не добавляем дубликат
        except Exception:
            pass
    log = UserLog(telegram_user_id=telegram_user_id, action=action, details=details, source=source, status=status)
    session.add(log)
    session.commit()
    # Очистка старых логов, оставить последние 1500
    old_logs = session.query(UserLog).order_by(UserLog.timestamp.desc()).offset(1500).all()
    if old_logs:
        for old in old_logs:
            session.delete(old)
        session.commit()

## Миграционные helper'ы для referral убраны по запросу: теперь ожидается, что схема уже приведена вручную.

