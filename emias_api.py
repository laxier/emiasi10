from datetime import datetime

import requests, json
from typing import Optional, Dict, Any
from database import get_db_session, get_tokens, save_tokens, get_profile, log_user_action


def get_specialities_info(user_id: int) -> list:
    url = "https://emias.info/api-eip/v6/saOrchestrator/getSpecialitiesInfo"
    session = get_db_session()
    profile = get_profile(session, user_id)
    if not profile:
        print("Не удалось получить данные (OMS/birthDate) из БД.")
        session.close()
        return None
    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
    }
    response = emias_post_request(user_id=user_id, url=url, payload=payload)
    return response.get("payload") if response else None


def is_token_expired(expires_at):
    """
    Проверяем, просрочен ли токен.
    expires_at — это datetime-объект из БД.
    """
    return datetime.now() >= expires_at


def refresh_emias_token(user_id: int, source: str = 'system') -> str:
    """
    Обновляет access_token для пользователя, используя refreshToken, сохранённый в БД.

    Отправляет POST-запрос на https://emias.info/web-api/refreshTokens/ с телом:
    {
        "refreshToken": "<текущий refreshToken>"
    }

    В ответе ожидается JSON:
    {
        "access_token": "<новый access_token>",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "openid profile",
        "refresh_token": "<новый refresh_token>"
    }

    Если обновление успешно, функция обновляет данные в БД и возвращает новый access_token,
    иначе возвращает None.
    """
    session = get_db_session()
    tokens = get_tokens(session, user_id)

    if tokens is None:
        session.close()
        print(f"[refresh_emias_token] Токены для пользователя {user_id} не найдены.")
        return None

    # Извлекаем текущий refresh_token из БД
    _, refresh_token, _ = tokens

    url = "https://emias.info/web-api/refreshTokens/"
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "refreshToken": refresh_token
    }

    try:
        print(f"[refresh_emias_token] POST {url} user={user_id}")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()

        new_access_token = data.get("access_token")
        new_refresh_token = data.get("refresh_token")
        expires_in = data.get("expires_in", 3600)  # время жизни токена в секундах

        if new_access_token and new_refresh_token:
            # Сохраняем новые токены в базу данных
            save_tokens(session, user_id, new_access_token, new_refresh_token, expires_in)
            # Если источник явно не указан вызывающим кодом (system), предполагаем что чаще это бот
            log_source = source if source else 'system'
            log_user_action(session, user_id, 'api_refresh_token', 'Токены обновлены', source=log_source, status='success')
            return new_access_token
        else:
            msg = f"Некорректный ответ при обновлении токена: {data}"
            print(msg)
            log_user_action(session, user_id, 'api_refresh_token', msg, source=source or 'system', status='error')
            return None
    except requests.exceptions.RequestException as e:
        # Попробуем извлечь тело ответа, если есть
        body = None
        try:
            if hasattr(e, 'response') and e.response is not None:
                body = e.response.text[:500]
        except Exception:
            body = None
        err = f"Ошибка при обновлении токена: {e}"
        if body:
            err += f" | body: {body}"
        print(err)
        # Первая попытка логирования в текущей сессии
        logged = False
        try:
            log_user_action(session, user_id, 'api_refresh_token', err, source=source or 'system', status='error')
            logged = True
        except Exception as le:
            print(f"log_user_action failed in refresh_emias_token primary session: {le}")
        # Резервная попытка с новой сессией
        if not logged:
            try:
                fallback_sess = get_db_session()
                log_user_action(fallback_sess, user_id, 'api_refresh_token', err, source=source or 'system', status='error')
                fallback_sess.close()
            except Exception as le2:
                print(f"Fallback logging failed: {le2}")
        return None
    finally:
        session.close()


def emias_post_request(
        user_id: int,
        url: str,
        payload: dict,
        timeout: int = 10
) -> Optional[dict]:
    """
    Универсальная функция для отправки POST-запроса к ЭМИАС.
    Она:
      1) Получает токен из БД по user_id.
      2) Проверяет, не просрочен ли токен (через is_token_expired).
      3) Если просрочен, то вызывает refresh_emias_token.
      4) Формирует заголовки (ei-token) и выполняет POST-запрос.
      5) Возвращает ответ (JSON) либо None при ошибке.

    :param user_id: Идентификатор пользователя (нужен для получения/обновления токена).
    :param url: Эндпоинт, куда делается запрос.
    :param payload: Тело запроса (JSON).
    :param timeout: Таймаут запроса в секундах (по умолчанию 10).
    :return: Распарсенный JSON (словарь) или None в случае ошибки.
    """
    session = get_db_session()
    tokens = get_tokens(session, user_id)

    if not tokens:
        session.close()
        print("Не найдены токены для данного пользователя.")
        return None

    access_token, _, expires_at = tokens

    # Проверяем, не истёк ли срок действия access_token
    if is_token_expired(expires_at):
        # Пытаемся обновить
        new_token = refresh_emias_token(user_id, source='system')
        if not new_token:
            from database import log_user_action
            try:
                log_user_action(session, user_id, 'api_refresh_token', 'Не удалось обновить токен (просрочен, требуется /auth)', source='system', status='error')
            except Exception:
                pass
            session.close()
            print("Не удалось обновить токен.")
            return None
        access_token = new_token

    # Теперь токен точно актуален
    headers = {
        "ei-token": access_token,
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_message = f"Ошибка при запросе {url}"
        error_description = None
        try:
            # Попытка получить дополнительную информацию из ответа, если он есть
            error_description = response.json().get("error", {}).get("description")
            error_message += f"\nОписание: {error_description}"
        except (NameError, AttributeError, ValueError):
            # NameError: если response не был присвоен
            # AttributeError/ValueError: если response.json() не удался или не содержит нужных ключей
            pass
        
        print(error_message)
        print(f"Payload: {payload}")
        return {"Описание": error_description or "Неизвестная ошибка"}
    finally:
        session.close()


def get_whoami(user_id: int) -> dict:
    url = "https://emias.info/web-api/whoAmI/"
    payload = {
        "accessToken": ""  # В whoAmI есть поле "accessToken", но оно продублируется – обычно оно не критично
    }
    return emias_post_request(user_id=user_id, url=url, payload=payload)


def get_assignments_referrals_info(user_id: int) -> dict:
    url = "https://emias.info/api-eip/v2/saOrchestrator/getAssignmentsReferralsInfo"
    session = get_db_session()
    profile = get_profile(session, user_id)
    if not profile:
        print("Не удалось получить данные (OMS/birthDate) из БД.")
        session.close()
        return None
    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
    }
    return emias_post_request(user_id=user_id, url=url, payload=payload).get("payload")

def sync_referrals_to_links(user_id: int) -> int:
    """Получает getAssignmentsReferralsInfo и сохраняет referralId в UserDoctorLink по speciality.

    Возвращает количество обновлённых ссылок.
    """
    data = get_assignments_referrals_info(user_id)
    if not data:
        return 0
    ar_info = data.get("arInfo", {})
    referrals = ar_info.get("referrals", {}).get("items", []) or []
    # Ищем пары specialityId -> referralId
    updates = 0
    try:
        from database import get_db_session, UserDoctorLink
        sess = get_db_session()
        try:
            for item in referrals:
                ref_id = item.get("referralId") or item.get("id")
                spec_id = item.get("specialityId") or item.get("specialityCode")
                if not ref_id or not spec_id:
                    continue
                link = sess.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=str(spec_id)).first()
                if link and link.referral_id != str(ref_id):
                    link.referral_id = str(ref_id)
                    updates += 1
            if updates:
                sess.commit()
        finally:
            sess.close()
    except Exception:
        return updates
    return updates


def get_appointment_receptions_by_patient(user_id: int) -> dict:
    url = "https://emias.info/api-eip/v8/saOrchestrator/getAppointmentReceptionsByPatient"
    session = get_db_session()
    profile = get_profile(session, user_id)
    if not profile:
        print("Не удалось получить данные (OMS/birthDate) из БД.")
        session.close()
        return None
    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
    }
    response = emias_post_request(user_id=user_id, url=url, payload=payload)
    # Убрано подробное логирование количества записей (только создание/перенос пишем в логи)
    return response.get("payload") if response else None


from database import get_profile


def get_specialities_info(user_id: int) -> list:
    url = "https://emias.info/api-eip/v6/saOrchestrator/getSpecialitiesInfo"
    session = get_db_session()
    profile = get_profile(session, user_id)
    if not profile:
        print("Не удалось получить данные (OMS/birthDate) из БД.")
        session.close()
        return None
    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "isChatBotEnabled": False
    }
    response = emias_post_request(user_id=user_id, url=url, payload=payload)
    return response.get("payload") if response else None


import requests
from typing import Optional, List, Union
from database import get_db_session, get_tokens


from typing import Optional, Tuple
from sqlalchemy.orm import Session
from database import DoctorInfo, Specialty

from typing import Union, Tuple

def resolve_inquiry_purpose_codes(
    session: Session,
    available_resource_id: int,
) -> Tuple[Union[int, str], Union[int, str]]:
    """
    Возвращает inquiry_purpose_code и inquiry_purpose_id по available_resource_id.

    Если в базе данных значение NULL — возвращает "" вместо None.
    :return: (inquiry_purpose_code, inquiry_purpose_id) — могут быть int или ""
    """
    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=available_resource_id).first()
    if doctor and doctor.ar_speciality_id:
        specialty = session.query(Specialty).filter_by(code=doctor.ar_speciality_id).first()
        if specialty:
            return (
                specialty.ar_inquiry_purpose_code if specialty.ar_inquiry_purpose_code is not None else "",
                specialty.ar_inquiry_purpose_id if specialty.ar_inquiry_purpose_id is not None else ""
            )
    return "", ""

def get_doctors_info(
    user_id: int,
    speciality_id: Optional[List[str]] = None,
    referral_id: Optional[Union[str, int]] = None,
    appointment_id: Optional[Union[str, int]] = None,
    lpu_id: Optional[Union[str, int]] = None
) -> Optional[dict]:
    """
    Функция-обёртка для запроса к эндпоинту /getDoctorsInfo.
    """

    session = get_db_session()
    profile = get_profile(session, user_id)
    if not profile:
        print("Не удалось получить данные (OMS/birthDate) из БД.")
        session.close()
        return None

    # Определяем inquiryPurposeId по speciality_id
    from database import Specialty
    inquiry_purpose_id = 61  # fallback значение

    if speciality_id and len(speciality_id) == 1:
        specialty = session.query(Specialty).filter_by(code=speciality_id[0]).first()
        if specialty and specialty.ar_inquiry_purpose_id:
            inquiry_purpose_id = specialty.ar_inquiry_purpose_id
    print(f"Используем inquiry_purpose_id: {inquiry_purpose_id} для speciality_id: {speciality_id}")
    session.close()

    url = "https://emias.info/api-eip/v4/saOrchestrator/getDoctorsInfo"
    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "specialityId": speciality_id if speciality_id is not None else [],
        "inquiryPurposeId": inquiry_purpose_id,
        "referralId": referral_id,
        "appointmentId": appointment_id,
        "lpuId": lpu_id
    }

    return emias_post_request(user_id, url, payload)


def get_lpus_for_speciality(user_id: int, speciality_code: str) -> Optional[dict]:
    """
    Делает запрос к эндпоинту /getLpusForSpeciality, передавая:
    {
      "omsNumber": <из профиля пользователя>,
      "birthDate": <из профиля пользователя>,
      "specialityCode": speciality_code
    }
    Возвращает полный распарсенный JSON или None при ошибке.
    """
    # Предположим, у нас есть функция get_profile()
    # для получения из базы omsNumber и birthDate по user_id:
    session = get_db_session()
    profile = get_profile(session, user_id)
    session.close()
    if not profile:
        print("Не найден профиль пользователя, не можем получить omsNumber/birthDate")
        return None

    url = "https://emias.info/api-eip/v3/saOrchestrator/getLpusForSpeciality"
    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "specialityCode": speciality_code
    }

    return emias_post_request(user_id, url, payload)


def create_appointment(
    user_id: int,
    available_resource_id: int,
    complex_resource_id: int,
    start_time: str,
    end_time: str,
    reception_type_id: int,
    inquiry_purpose_code: Optional[int] = None,
    inquiry_purpose_id: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    """
    Создает новую запись к врачу
    """
    url = "https://emias.info/api-eip/v3/saOrchestrator/createAppointment"

    session = get_db_session()
    profile = get_profile(session, user_id)

    if not profile:
        print("Не найден профиль пользователя: нет omsNumber/birthDate.")
        session.close()
        return None

    # Получаем коды из specialty при необходимости
    if inquiry_purpose_code is None or inquiry_purpose_id is None:
        inquiry_purpose_code, inquiry_purpose_id = resolve_inquiry_purpose_codes(session, available_resource_id)

    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "availableResourceId": available_resource_id,
        "complexResourceId": complex_resource_id,
        "startTime": start_time,
        "endTime": end_time,
        "receptionTypeId": reception_type_id,
        "inquiryPurposeCode": inquiry_purpose_code,
        "inquiryPurposeId": inquiry_purpose_id
    }
    # referralId отключён

    return emias_post_request(user_id, url, payload)

def get_available_resource_schedule_info(
    user_id: int,
    available_resource_id: int,
    complex_resource_id: int,
    appointment_id: Optional[int] = None,
    inquiry_purpose_code: Optional[int] = None,
    inquiry_purpose_id: Optional[int] = None
) -> Optional[dict]:
    """
    Делает запрос к /getAvailableResourceScheduleInfo, возвращая JSON-ответ
    с расписанием врача и данными по ресурсу.
    """
    url = "https://emias.info/api-eip/v3/saOrchestrator/getAvailableResourceScheduleInfo"

    session = get_db_session()
    profile = get_profile(session, user_id)

    if not profile:
        print("Не найден профиль пользователя: нет omsNumber/birthDate.")
        session.close()
        return None

    if (inquiry_purpose_code is None or inquiry_purpose_id is None) and appointment_id is None:
        inquiry_purpose_code, inquiry_purpose_id = resolve_inquiry_purpose_codes(session, available_resource_id)

    session.close()

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "availableResourceId": available_resource_id,
        "complexResourceId": complex_resource_id,
    **({"appointmentId": int(appointment_id) if isinstance(appointment_id, str) else appointment_id} if appointment_id else {
        "inquiryPurposeId": inquiry_purpose_id
    })
}
    response = emias_post_request(user_id, url, payload)

    # Автосохранение расписания в doctor_schedules при любом успешном ответе с scheduleOfDay
    try:
        if response and response.get("payload") is not None:
            schedule_days = response.get("payload", {}).get("scheduleOfDay")
            if schedule_days is not None:  # даже пустой список сохраняем, чтобы в веб не было "Нет сохранённого"
                from database import DoctorSchedule, DoctorInfo  # get_db_session уже импортирован модулем
                sess = get_db_session()
                # Убедимся что есть запись о враче (FK). Если нет — пропускаем сохранение.
                doctor_exists = sess.query(DoctorInfo).filter_by(doctor_api_id=str(available_resource_id)).first()
                if doctor_exists:
                    rec = sess.query(DoctorSchedule).filter_by(doctor_api_id=str(available_resource_id)).first()
                    serialized = json.dumps(schedule_days, ensure_ascii=False)
                    now_dt = datetime.utcnow()
                    if rec:
                        rec.schedule_text = serialized
                        # updated_at может быть TEXT в БД — сохраняем ISO строку для совместимости
                        try:
                            rec.updated_at = now_dt
                        except Exception:
                            rec.updated_at = now_dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        try:
                            rec = DoctorSchedule(
                                doctor_api_id=str(available_resource_id),
                                schedule_text=serialized,
                                updated_at=now_dt
                            )
                        except Exception:
                            rec = DoctorSchedule(
                                doctor_api_id=str(available_resource_id),
                                schedule_text=serialized,
                                updated_at=now_dt.strftime('%Y-%m-%d %H:%M:%S')
                            )
                        sess.add(rec)
                    sess.commit()
                sess.close()
    except Exception as e:
        # Тихо логировать в stdout чтобы не ломать основной поток
        print(f"[WARN] Не удалось автосохранить расписание для {available_resource_id}: {e}")
    return response

def shift_appointment(
    user_id: int,
    available_resource_id: int,
    complex_resource_id: int,
    start_time: str,
    end_time: str,
    appointment_id: int,
    reception_type_id: int
) -> Optional[Dict[str, Any]]:
    """
    Переносит существующую запись на новое время
    """
    url = "https://emias.info/api-eip/v3/saOrchestrator/shiftAppointment"
    
    session = get_db_session()
    profile = get_profile(session, user_id)
    session.close()

    if not profile:
        print("Не найден профиль пользователя: нет omsNumber/birthDate.")
        return None

    payload = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "availableResourceId": available_resource_id,
        "complexResourceId": complex_resource_id,
        "startTime": start_time,
        "endTime": end_time,
        "appointmentId": appointment_id,
        "receptionTypeId": reception_type_id
    }
    # referralId отключён

    response = emias_post_request(user_id, url, payload)
    if response is None:
        return None

    return response