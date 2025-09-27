import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, BotCommand
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from config import TELEGRAM_BOT_TOKEN
from database import init_db, get_db_session, save_tokens, get_tokens, save_profile, get_profile, get_equivalent_speciality_codes, UserDoctorLink, log_user_action, DoctorSchedule
from emias_api import get_whoami, refresh_emias_token, get_assignments_referrals_info
from rules_parser import parse_user_tracking_input

logging.basicConfig(level=logging.INFO)

# Специальности / типы (как строки specialityId или ldpTypeId), для которых можно записываться без направления
DISPENSARY_WHITELIST: set[str] = {"600034"}  # суточное мониторирование АД (СМАД)

# DEBUG: набор слотов, за которыми хотим специально наблюдать в логах.
# Формат элементов: "YYYY-MM-DD HH:MM".
DEBUG_SLOTS: set[str] = {
    # "2025-09-27 13:48",
}


# Определяем FSM для авторизации, когда пользователь вручную вводит токены
class AuthStates(StatesGroup):
    waiting_for_access_token = State()
    waiting_for_refresh_token = State()


# FSM для регистрации профиля
class ProfileStates(StatesGroup):
    waiting_for_oms_number = State()
    waiting_for_birth_date = State()
    waiting_for_tracking_days = State()
    waiting_for_auto_booking = State()
    editing_tracking_rules = State()  # новое состояние для редактирования без сброса


# Обработчик команды /start
async def start_handler(message: Message) -> None:
    await message.answer(
        "Здравствуйте! Это тестовый бот для работы с API ЕМИАС.\n"
        "Для авторизации введите /auth.\n"
        "Для регистрации профиля (ОМС и дата рождения) введите /register_profile.\n"
        "Для получения данных по профилю введите /get_profile_info."
    )


# Обработчик команды /auth – переводит пользователя в режим ввода токенов
async def auth_handler(message: Message, state: FSMContext) -> None:
    await message.answer("Пожалуйста, введите ваш access_token:")
    await state.set_state(AuthStates.waiting_for_access_token)


# Обработчик ввода access_token
async def access_token_handler(message: Message, state: FSMContext) -> None:
    access_token = message.text.strip()
    # Сохраняем access_token в данные состояния
    await state.update_data(access_token=access_token)
    await message.answer("Пожалуйста, введите ваш refresh_token:")
    await state.set_state(AuthStates.waiting_for_refresh_token)


# Обработчик ввода refresh_token
async def refresh_token_handler(message: Message, state: FSMContext) -> None:
    refresh_token = message.text.strip()

    data = await state.get_data()
    access_token = data.get("access_token")
    expires_in = 3600  # Время жизни токена в секундах (можно задать динамически)

    session = get_db_session()
    save_tokens(session, message.from_user.id, access_token, refresh_token, expires_in)
    session.close()

    await message.answer("Токены успешно сохранены!")
    await state.clear()


# Обработчик /register_profile – ввод OMS и даты рождения
async def register_profile_handler(message: Message, state: FSMContext) -> None:
    await message.answer("Введите ваш номер ОМС:")
    await state.set_state(ProfileStates.waiting_for_oms_number)


async def oms_number_handler(message: Message, state: FSMContext) -> None:
    oms_number = message.text.strip()
    await state.update_data(oms_number=oms_number)
    await message.answer("Введите вашу дату рождения (YYYY-MM-DD):")
    await state.set_state(ProfileStates.waiting_for_birth_date)


async def birth_date_handler(message: Message, state: FSMContext) -> None:
    birth_date = message.text.strip()
    data = await state.get_data()
    oms_number = data.get("oms_number")
    session = get_db_session()
    save_profile(session, message.from_user.id, oms_number, birth_date)
    session.close()
    await message.answer("Ваш профиль успешно сохранён!")
    await state.clear()


# Обработчик /set_password – устанавливает пароль для веб-доступа
async def set_password_handler(message: Message) -> None:
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /set_password <пароль>")
        return
    password = args[1]
    session = get_db_session()
    profile = session.query(UserProfile).filter_by(telegram_user_id=message.from_user.id).first()
    if profile:
        profile.password = password
        session.commit()
        await message.answer("Пароль для веб-доступа установлен!")
    else:
        await message.answer("Профиль не найден. Создайте профиль командой /register_profile.")
    session.close()


# Обработчик /get_password – показывает текущий пароль
async def get_password_handler(message: Message) -> None:
    session = get_db_session()
    profile = session.query(UserProfile).filter_by(telegram_user_id=message.from_user.id).first()
    if profile and profile.password:
        await message.answer(f"Ваш пароль для веб-доступа: {profile.password}")
    else:
        await message.answer("Пароль не установлен. Установите командой /set_password <пароль>")
    session.close()


# Обработчик /get_profile_info – получает данные из профиля и запрашивает информацию по API
from datetime import datetime, date, time, timedelta
from aiogram.types import Message
from database import get_db_session, get_tokens, get_profile, log_user_action
from emias_api import get_whoami


async def get_profile_info_handler(message: Message) -> None:
    """
    Обработчик команды /get_profile_info.
    Выводит информацию из профиля пользователя (номер полиса и дополнительные данные),
    а также отправляет запрос к API whoAmI с использованием access_token и выводит его ответ.
    """
    session = get_db_session()

    # Получаем профиль пользователя
    profile = get_profile(session, message.from_user.id)
    session.close()

    answer_text = ""

    if profile:
        answer_text += "Информация о вашем профиле:\n"
        answer_text += f"Номер полиса: {profile.oms_number if profile.oms_number else 'Не указано'}\n"
        answer_text += f"Дата рождения: {profile.birth_date}\n"
    else:
        answer_text += "Профиль не найден. Создайте профиль командой /create_profile.\n"

    data = get_whoami(message.from_user.id)

    if data:
        # Формируем ФИО, используя поля FirstName, middle_name и LastName
        first_name = data.get("FirstName", "")
        middle_name = data.get("middle_name", "")
        last_name = data.get("LastName", "")
        fio = f"{first_name} {middle_name} {last_name}".strip()
        gender = data.get("gender", "")
        answer_text += "\nОтвет от whoAmI:\n"
        answer_text += f"ФИО: {fio}\n"
        answer_text += f"Пол: {gender}\n"
    else:
        answer_text += "Ошибка при выполнении запроса к API whoAmI.\n"
    await message.answer(answer_text)


# Обработчик команды /whoami – использует сохранённые токены для запроса к API
async def whoami_handler(message: Message) -> None:
    """
    Обработчик команды /whoami:
    - Получает токены из базы для текущего пользователя.
    - Если access_token просрочен, пытается обновить его.
    - Отправляет запрос к API ЕМИАС и возвращает результат пользователю.
    """

    data = get_whoami(message.from_user.id)
    if data:
        answer_text = "Успешный ответ от сервера:\n"
        for key, value in data.items():
            answer_text += f"{key}: {value}\n"
        await message.answer(answer_text)
    else:
        await message.answer("Ошибка при выполнении запроса к API ЕМИАС.")


import asyncio
import logging
from aiogram.types import Message
from aiogram.filters import Command

from config import TELEGRAM_BOT_TOKEN
from database import init_db, get_db_session, get_tokens, get_profile
from emias_api import get_whoami, refresh_emias_token, get_appointment_receptions_by_patient, \
    get_assignments_referrals_info, get_specialities_info

logging.basicConfig(level=logging.INFO)


# Обработчик команды /get_receptions – получение данных о приёмах
def format_appointment(appt: dict) -> str:
    """
    Формирует строку с информацией о записи (appointment).
    Выводит номер, ЛПУ (название и адрес), время начала и окончания.
    Если запись типа RECEPTION, выводится информация о враче.
    Если запись типа BM, выводится название услуги и лабораторий из первого элемента registryEntry.
    """
    number = appt.get("id", "Нет данных")
    name_lpu = appt.get("nameLpu", "Нет данных")
    lpu_address = appt.get("lpuAddress", "Нет данных")
    start_time = humanize_datetime(appt.get("startTime", "Нет данных"))
    end_time = humanize_datetime(appt.get("endTime", "Нет данных"))
    appointment_type = appt.get("type", "Нет данных")

    # result = f"ID записи: {number}\n"
    result = f"{name_lpu} - {lpu_address}\n"
    result += f"{start_time}\n"


    if appointment_type == "RECEPTION":
        to_doctor = appt.get("toDoctor", {})
        doctor_fio = to_doctor.get("doctorFio", "Нет данных")
        doctor_specialty = to_doctor.get("specialityName", "Врач")
        result += f"{doctor_specialty}: {doctor_fio}\n"
    elif appointment_type == "BM":
        to_bm = appt.get("toBM", {})
        service_name = to_bm.get("name", "Нет данных")
        registry_entries = to_bm.get("registryEntry", [])
        lab_names = []
        if registry_entries:
            for entry in registry_entries:
                lab_name = entry.get("laboratoryName", "Нет данных")
                lab_names.append(lab_name)
        if lab_names:
            result += f"Услуга: {service_name}\n"
            result += f"Анализ: {', '.join(lab_names)}\n"
        else:
            result += f"Услуга: {service_name}\n"
    elif appointment_type == "LDP":
        to_ldp = appt.get("toLdp", {})
        service = to_ldp.get("ldpTypeName", "—")
        result += f"Услуга: {service}\n"
        room_number = appt.get("roomNumber", "—")
        result += f"Кабинет: {room_number}\n"
        result += f"Время: {start_time} - {end_time}\n"
    return result + "\n"


from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database import Specialty, UserProfile
async def get_receptions_handler(message: Message) -> None:
    """
    Обработчик получения информации о приёмах пациента.
    Отправляет каждый приём отдельным сообщением и добавляет кнопку "Перенести" для RECEPTION и LDP.
    При наличии данных о враче (например, availableResourceId) сохраняет/обновляет информацию о враче,
    а также связывает специальность с пользователем.
    """
    session = get_db_session()
    try:
        tokens = get_tokens(session, message.from_user.id)
        profile = get_profile(session, message.from_user.id)

        if not tokens:
            await message.answer("Токены не найдены. Авторизуйтесь через /auth.")
            return
        if not profile:
            await message.answer("Профиль не найден. Создайте профиль командой /create_profile.")
            return

        # Получаем список приёмов
        data = get_appointment_receptions_by_patient(message.from_user.id)

        if not data:
            await message.answer("Ошибка при выполнении запроса к API getAppointmentReceptionsByPatient.")
            return

        appointments = data.get("appointment", [])
        if not appointments:
            await message.answer("Записей не найдено.")
            return

        for appt in appointments:
            appt_text = format_appointment(appt)  # Форматируем текст приёма
            buttons = []  # Список кнопок

            appt_id = appt.get("id", "unknown")
            # Если приём можно перенести, добавляем кнопку "Перенести"
            if appt.get("enableShift"):
                buttons.append(InlineKeyboardButton(
                    text="🔄 Перенести приём",
                    callback_data=f"reschedule:{appt_id}"
                ))

            # Подготовка данных врача для сохранения/обновления
            doctor_api_id = appt.get("availableResourceId")
            doctor_data = None

            # Специальный случай: LDP (например, суточное мониторирование АД) иногда не имеет availableResourceId.
            # Чтобы сохранить привязку (для отображения и последующего маппинга specialty/referral), создаём синтетического "врача".
            # Унифицированная обработка: сначала пытаемся собрать по ldpType (для LDP), иначе по toDoctor (RECEPTION)
            appt_type = appt.get("type")
            if appt_type == "LDP":
                ldp_block = appt.get("toLdp", {}) or {}
                ldp_type_id = ldp_block.get("ldpTypeId")
                ldp_type_name = ldp_block.get("ldpTypeName") or "Диагностика"
                # Если нет resourceId – синтетический ID
                if not doctor_api_id and ldp_type_id:
                    doctor_api_id = f"ldp:{ldp_type_id}"
                if doctor_api_id:
                    doctor_data = {
                        "id": doctor_api_id,
                        "name": appt.get("doctorName") or appt.get("roomNumber") or ldp_type_name or (f"LDP {ldp_type_id}" if ldp_type_id else "Диагностика"),
                        # Явно заполняем arSpecialityId/Name значениями из ldpType, чтобы везде использовать один ключ
                        # (например, для связок UserDoctorLink, поиска расписания и т.п.)
                        "arSpecialityId": appt.get("arSpecialityId") or ldp_type_id,
                        "arSpecialityName": appt.get("arSpecialityName") or ldp_type_name,
                        "complexResource": appt.get("complexResource", []) or [],
                        "ldpType": ([{
                            "code": str(ldp_type_id),
                            "name": ldp_type_name
                        }] if ldp_type_id else []),
                        "appointment_id": appt.get("id")
                    }
            elif appt_type == "RECEPTION" and doctor_api_id:
                to_doctor = appt.get("toDoctor", {})
                doctor_data = {
                    "id": doctor_api_id,
                    "name": to_doctor.get("doctorFio", "Неизвестный врач"),
                    "arSpecialityId": to_doctor.get("specialityId"),
                    "arSpecialityName": to_doctor.get("specialityName"),
                    "complexResource": [],
                    "ldpType": [],
                    "appointment_id": appt.get("id")
                }

            # Если сформированы данные врача, сохраняем/обновляем их в БД
            if doctor_data:
                save_or_update_doctor(session, message.from_user.id, doctor_data)

            # Создаем клавиатуру, если есть кнопки
            keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons]) if buttons else None
            await message.answer(appt_text, reply_markup=keyboard)
        
        session.commit()
    finally:
        session.close()


from aiogram.types import CallbackQuery
from typing import Optional, List, Union


async def process_reschedule(callback_query: CallbackQuery):
    try:
        # Разбиваем callback_data с учетом возможных ошибок
        data_parts = callback_query.data.split(":")
        if len(data_parts) < 2:
            await callback_query.answer("Некорректные данные!", show_alert=True)
            return
        elif len(data_parts) == 2:
            appt_id = data_parts[1]
            lpu_id = None
        elif len(data_parts) == 3:
            appt_id = data_parts[1]
            # lpu_id support removed

        user_id = callback_query.from_user.id  # ID пользователя Telegram

        payload = get_doctors_info(
            user_id=user_id,
            speciality_id=[""],
            appointment_id=appt_id,
            # lpu_id removed
        ).get("payload")

        doctors_info = payload.get("doctorsInfo", [])
        not_available_doctors = payload.get("notAvailableDoctors", [])

        # Сохраняем данные о врачах
        session = get_db_session()
        # Доступные ресурсы
        for block in doctors_info:
            for resource in block.get("availableResources", []):
                save_or_update_doctor(session, callback_query.from_user.id, resource)
        # Недоступные врачи
        for doc in not_available_doctors:
            save_or_update_doctor(session, callback_query.from_user.id, doc)
        session.commit()

        # --- Отправляем информацию о доступных врачах ---
        if doctors_info:
            await callback_query.message.answer("<b>Доступные врачи:</b>", parse_mode="HTML")
            for block in doctors_info:
                # Removed LPU name output
                resources = block.get("availableResources", [])
                if not resources:
                    await callback_query.message.answer("   Нет доступных ресурсов.")
                else:
                    for resource in resources:
                        resource_id = resource.get("id", "???")
                        doc_name = resource.get("name", "Неизвестный врач")
                        msg_text = f"👨‍⚕️ Врач: {doc_name}"
                        ldp_types = resource.get("ldpType", [])
                        if ldp_types:
                            speciality_info = ldp_types[0]
                        c_id = None
                        for c_res in resource.get("complexResource", []):
                            c_id = c_res.get("id", "???")
                            break
                        kb = build_doctor_toggle_keyboard(session, callback_query.from_user.id, str(resource_id))
                        reschedule_btn = InlineKeyboardButton(
                            text="🔄 Перенести сюда",
                            callback_data=f"do_reschedule:{appt_id}:{resource_id}:{c_id}"
                        )
                        kb.inline_keyboard.append([reschedule_btn])
                        schedule_response = get_available_resource_schedule_info(
                            user_id, resource_id, c_id, appt_id
                        )
                        if schedule_response and schedule_response.get("payload"):
                            schedule_text = "\n\n" + format_schedule_message_simple(schedule_response.get("payload"))
                        else:
                            error_desc = schedule_response.get("Описание") if schedule_response else None
                            if not error_desc and schedule_response and schedule_response.get("payload"):
                                error_desc = schedule_response.get("payload").get("Описание")
                            schedule_text = "\n\n" + (error_desc or "Расписание недоступно")
                        msg_text += schedule_text
                        msg_text = safe_html(msg_text)
                        await callback_query.message.answer(msg_text, reply_markup=kb, parse_mode="HTML")
        else:
            await callback_query.message.answer("<b>Нет доступных врачей.</b>", parse_mode="HTML")

        # --- Отправляем информацию о недоступных врачах ---
        if not_available_doctors:
            await callback_query.message.answer("<b>Недоступные врачи:</b>", parse_mode="HTML")
            for doc in not_available_doctors:
                doc_id = doc.get("id", "???")
                doc_name = doc.get("name", "???")
                msg_text = f"   - {doc_name}"
                msg_text = safe_html(msg_text)
                kb = build_doctor_toggle_keyboard(session, callback_query.message.from_user.id, str(doc_id))
                await callback_query.message.answer(text=msg_text, reply_markup=kb, parse_mode="HTML")
        else:
            await callback_query.message.answer("Нет недоступных врачей.", parse_mode="HTML")

    except Exception as e:
        await callback_query.message.answer(f"Ошибка обработки: {str(e)}")


async def book_slot_callback(callback_query: CallbackQuery):
    """
    Обработчик для записи на выбранный слот.
    callback_data: "book_slot:{doctor_api_id}:{slot}"
    """
    try:
        data_parts = callback_query.data.split(":", 2)
        if len(data_parts) != 3:
            await callback_query.answer("Некорректные данные!", show_alert=True)
            return
        _, doctor_api_id, slot = data_parts
        user_id = callback_query.from_user.id

        # Попытаться записаться
        success, error_msg = await book_appointment(user_id, doctor_api_id, slot)
        from database import get_db_session, log_user_action, UserTrackedDoctor
        if success:
            # Отключаем автозапись, поскольку пользователь вручную выбрал слот
            session = get_db_session()
            tracking = session.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_api_id).first()
            if tracking and tracking.auto_booking:
                tracking.auto_booking = False
                session.commit()
            log_user_action(session, user_id, 'manual_booking', f'Доктор {doctor_api_id} слот {slot}', source='bot', status='success')
            session.close()
            await callback_query.message.edit_text(
                f"✅ Успешно записались на {slot}!",
                parse_mode="HTML"
            )
        else:
            session = get_db_session()
            log_user_action(session, user_id, 'manual_booking_fail', f'Доктор {doctor_api_id} слот {slot} ошибка: {error_msg}', source='bot', status='error')
            session.close()
            await callback_query.answer(f"Не удалось записаться: {error_msg}", show_alert=True)
    except Exception as e:
        session = get_db_session()
        log_user_action(session, callback_query.from_user.id, 'manual_booking_exception', f'Ошибка: {e}', source='bot', status='error')
        session.close()
        await callback_query.answer(f"Ошибка: {str(e)}", show_alert=True)


async def do_reschedule_callback(callback_query: CallbackQuery):
    """
    Обработчик для переноса приёма на выбранного врача/ресурс.
    callback_data: "do_reschedule:{appt_id}:{resource_id}:{c_id}"
    """
    try:
        data_parts = callback_query.data.split(":")
        if len(data_parts) != 4:
            await callback_query.answer("Некорректные данные!", show_alert=True)
            return
        _, appt_id_str, resource_id_str, c_id_str = data_parts
        appt_id = int(appt_id_str)
        resource_id = int(resource_id_str)
        c_id = int(c_id_str) if c_id_str != "None" else None
        user_id = callback_query.from_user.id

        # Найти самый ранний слот для этого ресурса
        schedule_response = get_available_resource_schedule_info(user_id, resource_id, c_id, appt_id)
        if not schedule_response or not schedule_response.get("payload"):
            await callback_query.answer("Не удалось получить расписание.", show_alert=True)
            return

        # Использовать логику из blood.py для поиска слота
        # Но упростить: взять первый доступный слот
        earliest_slot = None
        for day in schedule_response.get("payload").get("scheduleOfDay", []):
            for slot_block in day.get("scheduleBySlot", []):
                for slot in slot_block.get("slot", []):
                    start_time = slot.get("startTime")
                    end_time = slot.get("endTime")
                    if start_time and end_time:
                        earliest_slot = (start_time, end_time)
                        break
                if earliest_slot:
                    break
            if earliest_slot:
                break

        if not earliest_slot:
            await callback_query.answer("Нет доступных слотов.", show_alert=True)
            return

        start_time, end_time = earliest_slot

        # Получить reception_type_id (только из Specialty, т.к. в расписании его нет и не будет)
        reception_type_id = 0
        from database import get_db_session as _gdb, DoctorInfo, Specialty, log_user_action as _lua
        _sess = _gdb()
        try:
            doc = _sess.query(DoctorInfo).filter_by(doctor_api_id=str(resource_id)).first()
            if doc and doc.ar_speciality_id:
                spec = _sess.query(Specialty).filter_by(code=doc.ar_speciality_id).first()
                if spec and spec.reception_type_id not in (None, ""):
                    try:
                        reception_type_id = int(spec.reception_type_id)
                    except Exception:
                        reception_type_id = 0
                else:
                    try:
                        _lua(_sess, user_id, 'api_reception_type_missing_db', f'spec {doc.ar_speciality_id}', source='bot', status='info')
                    except Exception:
                        pass
        finally:
            _sess.close()

        # Вызвать shift_appointment
        from emias_api import shift_appointment
        response = shift_appointment(
            user_id=user_id,
            available_resource_id=resource_id,
            complex_resource_id=c_id,
            start_time=start_time,
            end_time=end_time,
            appointment_id=appt_id,
            reception_type_id=reception_type_id
        )

        # Критерий успеха: есть payload с данными или appointmentId (верхний уровень или внутри payload)
        success = False
        appointment_new_id = None
        if isinstance(response, dict):
            if 'appointmentId' in response:
                success = True
                appointment_new_id = response.get('appointmentId')
            elif response.get('payload') and isinstance(response.get('payload'), dict):
                inner = response.get('payload')
                if inner.get('appointmentId'):
                    success = True
                    appointment_new_id = inner.get('appointmentId')
        # Извлекаем детали для сообщения
        avail_res = schedule_response.get("payload", {}).get("availableResource", {})
        doctor_name = avail_res.get("name", "Врач")
        from database import log_user_action, get_db_session
        sess = None
        try:
            sess = get_db_session()
            if success:
                # Парсим дату/время
                from datetime import datetime
                months = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}
                try:
                    start_dt = datetime.fromisoformat(start_time)
                    date_str = f"{start_dt.day} {months.get(start_dt.month, start_dt.strftime('%B'))} {start_dt.year}"
                    time_str = f"{start_dt.strftime('%H:%M')} - {datetime.fromisoformat(end_time).strftime('%H:%M')}"
                except Exception:
                    date_str = start_time[:10] if start_time else "Неизвестно"
                    time_str = f"{start_time[11:16] if start_time else '??:??'} - {end_time[11:16] if end_time else '??:??'}"
                msg = (
                    f"✅ Приём перенесён!\n"
                    f"👨‍⚕️ {doctor_name}\n"
                    f"📅 {date_str}\n"
                    f"🕒 {time_str}"
                )
                await callback_query.message.answer(msg)
                await callback_query.answer("Приём перенесён успешно!", show_alert=True)
                try:
                    # Добавляем детализацию: resource/complex/appointment/новый appointmentId (если вернулся)
                    extra = f'docName="{doctor_name}" res={resource_id} cRes={c_id} apptOld={appt_id} apptNew={appointment_new_id or "?"} {start_time}->{end_time}'
                    log_user_action(sess, user_id, 'api_shift_manual', extra, source='bot', status='success')
                except Exception:
                    pass
            else:
                # Ошибка: вытаскиваем описание
                error_text = None
                if isinstance(response, dict):
                    error_text = response.get('Описание') or response.get('error') or response.get('errorDescription')
                if not error_text:
                    error_text = 'Сервис недоступен'
                await callback_query.answer(f"Не удалось перенести приём: {error_text}", show_alert=True)
                try:
                    extra = f'docName="{doctor_name}" res={resource_id} cRes={c_id} apptOld={appt_id} err={error_text} {start_time}->{end_time}'
                    log_user_action(sess, user_id, 'api_shift_manual_fail', extra, source='bot', status='error')
                except Exception:
                    pass
        finally:
            if sess:
                sess.close()
    except Exception as e:
        await callback_query.answer(f"Ошибка: {str(e)}", show_alert=True)


def format_referral(item: dict) -> str:
    """
    Формирует красивую строку с информацией о направлении.
    """
    ref_type = item.get("type", "Неизвестно")
    number = item.get("number", "—")
    # lpu_name removed
    start_time = item.get("startTime", "—")
    end_time = item.get("endTime", "—")
    comment = item.get("comment", "—")
    issued_doctor =  item.get("issuedDoctor", {}).get("specialityName", "Врач")+ " " + item.get("issuedDoctor", {}).get("fio", "—")
    print(item)
    diagnosis = item.get("diagnosis", {}).get("code", "")+ " " +item.get("diagnosis", {}).get("name", "—")

    if ref_type == "REF_TO_DOCTOR":
        to_doctor = item.get("toDoctor", {})
        service = f"{to_doctor.get('specialityName', '—')}"
    elif ref_type == "REF_TO_LDP":
        to_ldp = item.get("toLdp", {})
        service = to_ldp.get("ldpTypeName", "—")
    else:
        service = "—"

    result = (
    ""
        f"🩺 Услуга: {service}\n"
        f"‍⚕️ {issued_doctor}\n"
        f"💬 Диагноз: {diagnosis}\n"
        f"📝 Комментарий: {comment}\n"
    )
    return result


def format_assignment(item: dict) -> str:
    """
    Формирует строку с информацией о назначении.
    Выводит:
    - Название услуги (toBM.name)
    - Период (dateFrom - dateTo)
    - Статус доступности для записи
    - Для каждого элемента из registryEntry:
        - Название лаборатории
        - Диагноз (код и название)
        - ФИО врача, который назначил
    """
    # Получаем основную информацию о назначении
    to_bm = item.get("toBM", {})
    service_name = to_bm.get("name", "Нет данных")
    period = item.get("period", {})
    date_from = period.get("dateFrom", "Нет данных")
    date_to = period.get("dateTo", "Нет данных")

    # Определяем доступность записи
    appointment_available = item.get("appointmentAvailable", False)
    available_text = "Доступно для записи" if appointment_available else "Не доступно для записи"

    result = (
        f"Услуга: {service_name}\n"
        f"Период: {date_from} - {date_to}\n"
        f"Запись: {available_text}\n"
    )

    # Обрабатываем каждую запись в реестре назначений
    registry_entries = item.get("registryEntry", [])
    if registry_entries:
        for entry in registry_entries:
            lab_name = entry.get("laboratoryName", "Нет данных")
            diagnosis = entry.get("diagnosis", {})
            diagnosis_code = diagnosis.get("code", "Нет данных")
            diagnosis_name = diagnosis.get("name", "Нет данных")
            issued_doctor = entry.get("issuedDoctor", {})
            doctor_fio = issued_doctor.get("fio", "Нет данных")

            result += (
                f"{lab_name},  {diagnosis_code} - {doctor_fio}\n"
            )
    result += "\n"
    return result


async def get_referrals_handler(message: Message) -> None:
    # NOTE: After refactoring/removing referral logic, some Python builds incorrectly
    # treated get_db_session as a local (likely due to earlier in-function imports during
    # previous edits). Explicitly declare it global to avoid UnboundLocalError.
    global get_db_session  # ensure we use the imported function
    session = get_db_session()
    tokens = get_tokens(session, message.from_user.id)
    profile = get_profile(session, message.from_user.id)
    session.close()

    if not tokens:
        await message.answer("Токены не найдены. Авторизуйтесь через /auth.")
        return
    if not profile:
        await message.answer("Профиль не найден. Создайте профиль командой /create_profile.")
        return

    data = get_assignments_referrals_info(message.from_user.id)
    if data:
        ar_info = data.get("arInfo", {})
        assignments = ar_info.get("assignments", {}).get("items", [])
        referrals = ar_info.get("referrals", {}).get("items", [])
        answer_text = "Направления:\n\n"
        if assignments:
            answer_text += "Назначения:\n"
            for item in assignments:
                answer_text += format_assignment(item)
        else:
            answer_text += "Назначения отсутствуют.\n\n"
        if referrals:
            answer_text += "Направления:\n"
            for item in referrals:
                answer_text += format_referral(item) + "\n"
        else:
            answer_text += "Направления отсутствуют.\n"
    else:
        answer_text = "Ошибка при выполнении запроса к API getAssignmentsReferralsInfo."
    await message.answer(answer_text, parse_mode="HTML")


# ---- LDP (диагностика) агрегатор по ЛПУ и адресам ----
async def ldp_aggregate_handler(message: Message) -> None:
    """Команда /ldp_agg
    Собирает все текущие записи пользователя типа LDP и показывает агрегировано:
    LPU -> адрес -> список услуг (ldpTypeName) и кабинеты.

    Дополнительно подтягивает доступные (enableShift=false/true) различая их не нужно – просто отображаем.
    Если у пользователя нет LDP записей – сообщаем.
    """
    data = get_appointment_receptions_by_patient(message.from_user.id)
    if not data:
        await message.answer("Не удалось получить приёмы.")
        return
    appts = data.get("appointment", []) or data.get("appointments", []) or []
    ldp_list = [a for a in appts if a.get("type") == "LDP"]
    if not ldp_list:
        await message.answer("LDP (диагностических) записей не найдено.")
        return
    # Структура: {(lpuId, lpuName): {(addressString): [(service, room, start, end)]}}
    aggregated = {}
    for appt in ldp_list:
        lpu_id = appt.get("lpuId") or appt.get("lpuID") or appt.get("idLpu")
        lpu_name = appt.get("nameLpu") or appt.get("lpuShortName") or appt.get("lpuName") or "ЛПУ"
        key_lpu = (lpu_id, lpu_name)
        lpu_block = aggregated.setdefault(key_lpu, {})
        # адрес может быть в lpuAddress либо в объекте location/address*
        address = appt.get("lpuAddress") or appt.get("addressString") or appt.get("address") or "Адрес не указан"
        addr_block = lpu_block.setdefault(address, [])
        to_ldp = appt.get("toLdp", {}) or {}
        service = to_ldp.get("ldpTypeName") or appt.get("ldpTypeName") or "Услуга"
        room = appt.get("roomNumber") or appt.get("room") or "—"
        start = appt.get("startTime", "")
        end = appt.get("endTime", "")
        # Человеко читаемо
        try:
            start_h = humanize_datetime(start)
        except Exception:
            start_h = start
        try:
            end_h = humanize_datetime(end)
        except Exception:
            end_h = end
        addr_block.append((service, room, start_h, end_h))

    # Формируем ответ
    parts = ["<b>Ваши диагностические записи (LDP):</b>"]
    for (lpu_id, lpu_name), addr_map in aggregated.items():
        parts.append(f"\n🏥 <b>{safe_html(lpu_name)}</b> (ID: {lpu_id})")
        for address, entries in addr_map.items():
            parts.append(f"📍 {safe_html(address)}")
            for service, room, start_h, end_h in sorted(entries, key=lambda x: x[2]):
                parts.append(f" • {safe_html(service)} | Каб. {safe_html(room)} | {safe_html(start_h)}")
    text = "\n".join(parts)
    # Ограничим по 4000 символов
    if len(text) > 4000:
        chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for i, ch in enumerate(chunks):
            await message.answer(ch, parse_mode="HTML")
    else:
        await message.answer(text, parse_mode="HTML")


# Обработчик команды /get_specialities – получение информации о специальностях
# (Повторные импорты, появившиеся после серии правок, удалены для ясности.)
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


async def get_specialities_handler(message: Message) -> None:
    """
    Обработчик команды /get_specialities.
    Выводит список специальностей в формате Markdown для удобного копирования команд.
    """
    session = get_db_session()
    tokens = get_tokens(session, message.from_user.id)
    profile = get_profile(session, message.from_user.id)
    session.close()

    if not tokens:
        await message.answer("Токены не найдены. Авторизуйтесь через /auth.")
        return
    if not profile:
        await message.answer("Профиль не найден. Создайте профиль командой /create_profile.")
        return

    # Получаем данные о специальностях
    data = get_specialities_info(message.from_user.id)

    if data:
        lines = ["*Специальности:*"]
        for group in data:
            if isinstance(group, dict):
                group_title = group.get("title", "Без названия")
                lines.append(f"• *{group_title}*")  # Жирный заголовок группы
                specialities = group.get("specialities", [])

                if isinstance(specialities, list):
                    for spec in specialities:
                        if isinstance(spec, dict):
                            spec_code = spec.get("specialityCode", "Нет кода")
                            spec_name = spec.get("specialityName", "")
                            if spec_name != "":
                                lines.append(f"     {spec_name}")
                                lines.append(f"     `/get_clinics {spec_code}`")
                            else:
                                lines.append(f"     `/get_doctors_info {spec_code}`")

        answer_text = "\n".join(lines)
        await message.answer(answer_text, parse_mode="Markdown")
    else:
        await message.answer("Ошибка при выполнении запроса к API getSpecialitiesInfo.")


from datetime import datetime


def humanize_datetime(dt_str: str) -> str:
    """
    Преобразует строку времени в формате ISO (например, "2025-03-24T10:36:00")
    в строку вида "24 марта 2025, 10:36".
    """
    try:
        dt = datetime.fromisoformat(dt_str)
        # Если нужно выводить месяц на русском, необходимо установить соответствующую локаль.
        # Например, для Linux:
        # import locale
        # locale.setlocale(locale.LC_TIME, "ru_RU.UTF-8")
        return dt.strftime("%d %B %Y, %H:%M")
    except Exception as e:
        return dt_str


from aiogram.filters.command import CommandObject
from emias_api import get_doctors_info

DERMATOLOGY_CODES = {
    "2028": "Заболевание кожи (исключая новообразования кожи)",
    "2029": "Новообразование кожи",
    "2030": "Обследование для военкомата",
    "2032": "Получение справок и направлений"
}

import html

ALLOWED_TAGS = {'<b>', '</b>', '<i>', '</i>', '<u>', '</u>', '<s>', '</s>'}


def safe_html(text: str) -> str:
    """
    Экранирует текст, не затрагивая разрешённые HTML-теги.
    Здесь предполагается, что все разрешённые теги уже известны.
    """
    # Заменим разрешённые теги на маркеры
    placeholders = {}
    for tag in ALLOWED_TAGS:
        placeholder = f"__PLACEHOLDER_{hash(tag)}__"
        placeholders[placeholder] = tag
        text = text.replace(tag, placeholder)

    # Экранируем весь текст
    safe_text = html.escape(text)

    # Возвращаем разрешённые теги обратно, заменяя маркеры на исходные теги
    for placeholder, tag in placeholders.items():
        safe_text = safe_text.replace(placeholder, tag)

    return safe_text


from database import save_or_update_doctors
import html
from aiogram import Router, F
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery
)
from aiogram.filters import Command, CommandObject

from database import (
    get_db_session,
    save_or_update_doctor,
    add_favorite_doctor,
    UserDoctorLink
)
from emias_api import get_doctors_info

# Создаём роутер (потом include_router(...) в основном коде)
router = Router()


def safe_html(text: str) -> str:
    """
    Экранирует текст, не затрагивая разрешённые HTML-теги.
    Здесь предполагается, что все разрешённые теги уже известны.
    """
    ALLOWED_TAGS = {'<b>', '</b>', '<i>', '</i>', '<u>', '</u>', '<s>', '</s>', '<pre>', '</pre>', '<code>', '</code>'}
    placeholders = {}
    for tag in ALLOWED_TAGS:
        placeholder = f"__PLACEHOLDER_{hash(tag)}__"
        placeholders[placeholder] = tag
        text = text.replace(tag, placeholder)
    safe_text = html.escape(text)
    for placeholder, tag in placeholders.items():
        safe_text = safe_text.replace(placeholder, tag)
    return safe_text


from database import UserFavoriteDoctor


def is_favorite_doctor(session, telegram_user_id: int, doctor_api_id: str) -> bool:
    """
    Проверяем, есть ли этот doctor_api_id в избранном у пользователя.
    """
    fav = session.query(UserFavoriteDoctor).filter_by(
        telegram_user_id=telegram_user_id,
        doctor_api_id=doctor_api_id
    ).first()
    return fav is not None


def build_doctor_toggle_keyboard(session, user_id: int, doctor_api_id: str) -> InlineKeyboardMarkup:
    """
    Создает inline-клавиатуру для переключения избранного:
    если врач уже в избранном — предлагаем «Удалить из избранного»,
    если нет — «Добавить в избранное».
    """

    in_fav = is_favorite_doctor(session, user_id, doctor_api_id)

    if in_fav:
        text_btn = "Убрать из избранного"
    else:
        text_btn = "Добавить в избранное"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=text_btn,
                    callback_data=f"fav_toggle:{doctor_api_id}"
                )
            ]
        ]
    )
    return keyboard


from emias_api import get_available_resource_schedule_info

from datetime import datetime


def format_schedule_message_simple(schedule_payload: dict) -> str:
    """
    Формирует сообщение с расписанием на основе полученного payload.

    Формат сообщения:
      <b>Расписание:</b>
      15 марта: 08:30, 09:00, 09:15
      16 марта: 09:00, 09:15, 09:30

    Для каждого дня берутся все startTime из всех слотов,
    преобразуются к виду HH:MM и объединяются через запятую.
    Дата форматируется в виде "день месяц" (например, "15 марта").
    """
    lines = ["<b>Расписание:</b>"]

    # Словарь с русскими названиями месяцев в родительном падеже
    months = {
        1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
        7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
    }

    schedule_of_day = schedule_payload.get("scheduleOfDay", [])
    for day in schedule_of_day:
        date_str = day.get("date", "Неизвестная дата")
        # Пробуем преобразовать дату из формата YYYY-MM-DD в "день месяц"
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            formatted_date = f"{dt.day} {months.get(dt.month, date_str)}"
        except Exception:
            formatted_date = date_str

        times = []
        schedule_by_slot = day.get("scheduleBySlot", [])
        for slot_info in schedule_by_slot:
            slots = slot_info.get("slot", [])
            for slot in slots:
                start = slot.get("startTime", "")
                if len(start) >= 16:
                    # Из строки ISO-формата берем символы с 11 до 16 (HH:MM)
                    times.append(start[11:16])
                else:
                    times.append(start)
        times_str = ", ".join(times)
        lines.append(f"{formatted_date}: {times_str}")

    return "\n".join(lines)


@router.message(Command("get_doctors_info"))
async def get_doctors_info_handler(message: Message, command: CommandObject):
    """
    /get_doctors_info <speciality_id> [lpu_id]
    1) Первый аргумент (speciality_id) обязателен.
    2) Второй аргумент (lpu_id) учитывается только, если speciality_id в DERMATOLOGY_CODES.
    3) Иначе игнорируется.
    """

    args = command.args.split() if command.args else []
    if len(args) < 1:
        help_text = (
            "Пожалуйста, укажите speciality_id (обязательный) и, опционально, lpu_id.\n\n"
            "Примеры:\n"
            "  /get_doctors_info 2\n"
            "  /get_doctors_info 2 10000431\n"
            "  /get_doctors_info 2028 10000431\n\n"
            "Где «2» или «2028» — это код специальности, а «10000431» — ID поликлиники."
        )
        await message.answer(help_text)
        return

    speciality_id_str = args[0]
    lpu_id_str = None
    if speciality_id_str in DERMATOLOGY_CODES:
        if len(args) >= 2:
            lpu_id_str = args[1]
    else:
        if len(args) >= 2:
            await message.answer(
                f"Второй аргумент (lpu_id = {html.escape(args[1])}) будет проигнорирован, "
                f"так как speciality_id {html.escape(speciality_id_str)} не относится к дерматологии."
            )

    # Получаем данные
    data = get_doctors_info(
        user_id=message.from_user.id,
        speciality_id=[speciality_id_str],
        referral_id=None,
        appointment_id=None,
        lpu_id=lpu_id_str
    )
    if not data:
        await message.answer("Не удалось получить информацию о врачах.")
        return

    payload = data.get("payload", {})
    doctors_info = payload.get("doctorsInfo", [])
    not_available_doctors = payload.get("notAvailableDoctors", [])

    # Сохраняем данные о врачах
    session = get_db_session()
    # Доступные
    for block in doctors_info:
        for resource in block.get("availableResources", []):
            save_or_update_doctor(session, message.from_user.id, resource)
    # Недоступные
    for doc in not_available_doctors:
        save_or_update_doctor(session, message.from_user.id, doc)
    session.commit()

    # Сразу отправляем данные (без накопления text_lines)
    # --- Доступные ---
    if doctors_info:
        await message.answer("<b>Доступные врачи:</b>", parse_mode="HTML")

        for block in doctors_info:
            lpu_name = block.get("lpuShortName", "Без названия")
            await message.answer(f"🏥 {lpu_name}", parse_mode="HTML")

            resources = block.get("availableResources", [])
            if not resources:
                await message.answer("   Нет доступных ресурсов.")
            else:
                for resource in resources:
                    resource_id = resource.get("id", "???")
                    doc_name = resource.get("name", "Неизвестный врач")
                    msg_text = f"👨‍⚕️ Врач: {doc_name}"

                    # msg_text+=f"\n   - ResourceID: {resource_id}"
                    for c_res in resource.get("complexResource", []):
                        c_id = c_res.get("id", "???")
                        # msg_text+=f"\n   - ComplexResourceID: {c_id}"
                    kb = build_doctor_toggle_keyboard(session, message.from_user.id, str(resource_id))
                    
                    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=str(resource_id)).first()
                    if doctor:
                        schedule_payload = await get_schedule_for_doctor(session, message.from_user.id, doctor)
                        if schedule_payload:
                            schedule_text = "\n\n" + format_schedule_message_simple(schedule_payload)
                            msg_text += schedule_text
                    
                    msg_text = safe_html(msg_text)

                    await message.answer(msg_text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer("<b>Нет доступных врачей.</b>", parse_mode="HTML")

    # --- Недоступные ---
    if not_available_doctors:
        await message.answer("<b>Недоступные врачи:</b>", parse_mode="HTML")
        for doc in not_available_doctors:
            doc_id = doc.get("id", "???")
            doc_name = doc.get("name", "???")
            msg_text = f"   - {doc_name}"
            msg_text = safe_html(msg_text)
            # msg_text+=f"(ID: {doc_id})"

            # for c_res in doc.get("complexResource", []):
            #     c_id = str(c_res.get("id", "???"))
            #     msg_text+=(f"\n     ComplexResource: ID={c_id}")

            kb = build_doctor_toggle_keyboard(session, message.from_user.id, str(doc_id))
            await message.answer(
                text=msg_text,
                reply_markup=kb,
                parse_mode="HTML"
            )
    else:
        await message.answer("Нет недоступных врачей.", parse_mode="HTML")

    session.close()


from database import DoctorInfo


async def toggle_favorite_callback_handler(callback: CallbackQuery):
    """
    При нажатии кнопки "Добавить/Удалить" из избранного.
    callback_data: "fav_toggle:<doctor_api_id>"
    """
    doctor_api_id = callback.data.split(":", 1)[1]
    session = get_db_session()
    user_id = callback.from_user.id

    # Получаем данные о враче из БД для вывода имени
    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=doctor_api_id).first()
    doctor_name = doctor.name if doctor else doctor_api_id  # если не найден, используем id

    if is_favorite_doctor(session, user_id, doctor_api_id):
        from database import remove_favorite_doctor
        remove_favorite_doctor(session, user_id, doctor_api_id)
        session.commit()
        text_reply = f"Врач {doctor_name} удалён из избранного!"
        try:
            log_user_action(session, user_id, 'bot_favorite_remove', f'Доктор {doctor_name} ({doctor_api_id})', source='bot', status='warning')
        except Exception:
            pass
    else:
        from database import add_favorite_doctor
        add_favorite_doctor(session, user_id, doctor_api_id)
        session.commit()
        text_reply = f"Врач {doctor_name} добавлен в избранное!"
        try:
            log_user_action(session, user_id, 'bot_favorite_add', f'Доктор {doctor_name} ({doctor_api_id})', source='bot', status='success')
        except Exception:
            pass

    session.close()
    await callback.answer(text_reply, show_alert=True)


from aiogram import Router
from aiogram.types import Message
from aiogram.filters import Command, CommandObject
from emias_api import get_lpus_for_speciality


async def get_clinics_handler(message: Message, command: CommandObject):
    """
    Команда /get_clinics [speciality_code].

    Пример:
      /get_clinics 2028
    Если код не указан, предлагаем варианты.
    Если указан несуществующий, пишем об ошибке.
    Иначе выводим список клиник.
    """
    speciality_code = command.args  # То, что идёт после /get_clinics

    # 1. Если код не передан, покажем список доступных
    if not speciality_code:
        text_lines = [
            "Пожалуйста, укажите код специальности. Доступные варианты:"
        ]
        for code, desc in DERMATOLOGY_CODES.items():
            text_lines.append(f"• {code} – {desc}")
        text_lines.append("Пример: /get_clinics 2028")
        await message.answer("\n".join(text_lines))
        return

    # 2. Если код не из DERMATOLOGY_CODES — сообщаем об ошибке
    if speciality_code not in DERMATOLOGY_CODES:
        valid_codes = "\n".join(f"{c} – {d}" for c, d in DERMATOLOGY_CODES.items())
        await message.answer(
            f"Неверный specialityCode: {speciality_code}\n"
            "Доступные варианты:\n"
            f"{valid_codes}"
        )
        return

    # 3. Код валидный — делаем запрос
    data = get_lpus_for_speciality(user_id=message.from_user.id, speciality_code=speciality_code)
    if not data:
        await message.answer("Не удалось получить список клиник (вернулся пустой ответ).")
        return

    payload = data.get("payload", {})
    lpu_list = payload.get("lpu", [])

    # 4. Формируем ответ
    if not lpu_list:
        await message.answer("Клиники не найдены.")
        return

    text_lines = [
        f"{DERMATOLOGY_CODES[speciality_code]}"
    ]
    for item in lpu_list:
        lpu_id = item.get("id", "???")
        lpu_name = item.get("shortName", "Без названия")
        text_lines.append(f" • {lpu_name}")

        # Если нужно, показать адрес(а)
        addresses = item.get("address", [])
        for addr in addresses:
            address_str = addr.get("addressString", "Неизвестный адрес")
            text_lines.append(f"{address_str}")
        text_lines.append(f"`/get_doctors_info {speciality_code} {lpu_id}`")
        text_lines.append("")

    await message.answer("\n".join(text_lines), parse_mode="Markdown")


from database import list_favorite_doctors, UserDoctorLink
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import is_tracking_doctor, add_tracking_doctor, UserTrackedDoctor, DoctorInfo


def build_tracking_toggle_keyboard(session, user_id: int, doctor_api_id: str) -> InlineKeyboardMarkup:
    """
    Создает inline-клавиатуру для переключения отслеживания:
    если врач уже отслеживается — предлагаем «Прекратить отслеживание»,
    если нет — «Отслеживать расписание».
    """
    is_tracked = is_tracking_doctor(session, user_id, doctor_api_id)

    if is_tracked:
        text_btn = "Прекратить отслеживание"
    else:
        text_btn = "Начать отслеживание"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=text_btn,
                    callback_data=f"track_toggle:{doctor_api_id}"
                )
            ]
        ]
    )
    return keyboard


def build_tracked_doctor_keyboard(doctor_api_id: str, is_active: bool) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру для отслеживаемого врача: изменить автозапись, изменить правила, приостановить/возобновить отслеживание, прекратить отслеживание.
    """
    toggle_text = "⏸️ Приостановить" if is_active else "▶️ Возобновить"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗓️ Автозапись",
                    callback_data=f"change_auto:{doctor_api_id}"
                ),
                InlineKeyboardButton(
                    text="📜 Правила",
                    callback_data=f"change_rules:{doctor_api_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=f"toggle_active:{doctor_api_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑️ Удалить",
                    callback_data=f"track_toggle:{doctor_api_id}"
                )
            ]
        ]
    )
    return keyboard


from collections.abc import Mapping, Sequence

# набор возможных вариантов ключа
_SPECIALITY_KEYS = {
    "specialityId",
    "specialtyId",
    "doctorSpecialityId",
    "doctorSpecialtyId",
}

def _to_str_or_empty(value) -> str:
    if value is None:
        return ""
    try:
        return str(int(value))
    except (ValueError, TypeError):
        # если это не число (на всякий), просто str()
        return str(value).strip()

def extract_speciality_id_from_appointment(appt: dict) -> str:
    """Возвращает specialityId из записи, учитывая любые варианты вложенности/имен."""
    if not appt or not isinstance(appt, Mapping):
        return ""

    # Специальный случай диагностических процедур (LDP): используем ldpTypeId как псевдо-"специальность"
    if appt.get("type") == "LDP":
        to_ldp = appt.get("toLdp")
        if isinstance(to_ldp, Mapping):
            ldp_type_id = to_ldp.get("ldpTypeId")
            if ldp_type_id not in (None, "", 0):
                try:
                    return str(int(ldp_type_id))
                except (ValueError, TypeError):
                    return str(ldp_type_id)

    # 1) быстрые явные пути
    # верхний уровень
    for k in _SPECIALITY_KEYS:
        if k in appt and appt[k] not in (None, "", 0):
            return _to_str_or_empty(appt[k])

    # toDoctor
    to_doctor = appt.get("toDoctor")
    if isinstance(to_doctor, Mapping):
        for k in _SPECIALITY_KEYS:
            if k in to_doctor and to_doctor[k] not in (None, "", 0):
                return _to_str_or_empty(to_doctor[k])

    # referral
    referral = appt.get("referral")
    if isinstance(referral, Mapping):
        for k in _SPECIALITY_KEYS:
            if k in referral and referral[k] not in (None, "", 0):
                return _to_str_or_empty(referral[k])

    # 2) общий глубокий поиск (dicts/lists/tuples)
    stack = [appt]
    seen = set()
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))

        if isinstance(node, Mapping):
            # прямое попадание ключа
            for k in _SPECIALITY_KEYS:
                if k in node and node[k] not in (None, "", 0):
                    return _to_str_or_empty(node[k])
            # углубляемся
            for v in node.values():
                if isinstance(v, (Mapping, Sequence)) and not isinstance(v, (str, bytes)):
                    stack.append(v)

        elif isinstance(node, Sequence) and not isinstance(node, (str, bytes)):
            for v in node:
                if isinstance(v, (Mapping, Sequence)) and not isinstance(v, (str, bytes)):
                    stack.append(v)

    return ""


@router.message(Command("favourites"))
async def favourites_handler(message: Message):
    session = get_db_session()
    user_id = message.from_user.id
    
    # Получаем все избранные врачей
    favorite_links = list_favorite_doctors(session, user_id)
    
    if not favorite_links:
        await message.answer("У вас пока нет избранных врачей. Вы можете добавить их со страницы с информацией о враче.")
        session.close()
        return

    # Получаем все doctor_api_id из избранного
    favorite_doctor_api_ids = {fav.doctor_api_id for fav in favorite_links}

    # Получаем информацию о врачах из DoctorInfo
    favorite_doctors = session.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.in_(favorite_doctor_api_ids)).all()

    if not favorite_doctors:
        await message.answer("Не удалось найти информацию по избранным врачам.")
        session.close()
        return

    await message.answer("Расписание для избранных врачей:")

    for doctor in favorite_doctors:
        schedule_response = await get_schedule_for_doctor(session, user_id, doctor)

        # Клавиатура для избранного и отслеживания
        fav_keyboard = build_doctor_toggle_keyboard(session, user_id, doctor.doctor_api_id)
        tracking_keyboard = build_tracking_toggle_keyboard(session, user_id, doctor.doctor_api_id)
        combined_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                fav_keyboard.inline_keyboard[0],
                tracking_keyboard.inline_keyboard[0]
            ]
        )

        if schedule_response and schedule_response.get("payload"):
            schedule_text = format_schedule_message_simple(schedule_response.get("payload"))
            doctor_info_text = f"<b>{doctor.name}</b>\n{doctor.ar_speciality_name}"
            await message.answer(
                f"{doctor_info_text}\n{schedule_text}",
                reply_markup=combined_keyboard,
                parse_mode="HTML"
            )
        else:
            # Проверить, есть ли запись к этому врачу
            has_appointment = False
            for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                if link and link.appointment_id:
                    has_appointment = True
                    break
            error_desc = schedule_response.get("Описание") if schedule_response else None
            if not error_desc and schedule_response and schedule_response.get("payload"):
                error_desc = schedule_response.get("payload").get("Описание")
            msg = f"{doctor.name} ({doctor.ar_speciality_name}): {error_desc or 'Не удалось получить расписание для врача.'}"
            await message.answer(msg, reply_markup=combined_keyboard)

    session.close()


from aiogram.fsm.state import StatesGroup, State


class TrackSetup(StatesGroup):
    choosing_days = State()
    choosing_auto_booking = State()


# Команда /tracked — показать отслеживаемых врачей
async def tracked_handler(message: Message):
    session = get_db_session()
    user_id = message.from_user.id
    tracked = session.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id).all()
    if not tracked:
        await message.answer("Вы пока не отслеживаете ни одного врача.")
        session.close()
        return

    doctor_ids = [t.doctor_api_id for t in tracked]
    doctors = session.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.in_(doctor_ids)).all()
    track_by_id = {t.doctor_api_id: t for t in tracked}
    for doctor in doctors:
        tracking = track_by_id.get(doctor.doctor_api_id)
        rules_list = tracking.tracking_rules or []
        rules_formatted = "\n".join(f"  - {rule}" for rule in rules_list) if rules_list else "  - без ограничений"
        auto_status = "включена" if tracking and tracking.auto_booking else "выключена"
        active_status = "активно" if tracking and tracking.active else "неактивно"
        appointment_info = "Запись: нет"
        for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
            link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
            if link and link.appointment_id:
                appointment_info = f"Запись: есть (ID: {link.appointment_id})"
                break
        info = (
            f"👨‍⚕️ <b>Врач:</b> {doctor.name}\n"
            f"🏥 <b>Специальность:</b> {doctor.ar_speciality_name}\n"
            f"📋 <b>{appointment_info}</b>\n"
            f"🔄 <b>Автозапись:</b> {auto_status}\n"
            f"<b>Отслеживание:</b> {active_status}\n"
            f"📅 <b>Правила отслеживания:</b>\n{rules_formatted}"
        )
        info = safe_html(info)
        keyboard = build_tracked_doctor_keyboard(doctor.doctor_api_id, tracking.active)
        await message.answer(info, parse_mode="HTML", reply_markup=keyboard)
    session.close()


from aiogram.fsm.context import FSMContext


async def track_schedule_toggle_callback_handler(callback: CallbackQuery, state: FSMContext):
    """
    Если запись уже есть — удаляем (прекращаем).
    Если нет — создаём (начинаем) + спрашиваем о днях/часах.
    """
    doctor_api_id = callback.data.split(":")[1]
    session = get_db_session()
    user_id = callback.from_user.id

    tracking = session.query(UserTrackedDoctor).filter_by(
        telegram_user_id=user_id, doctor_api_id=doctor_api_id
    ).first()

    if tracking:
        # Прекращаем отслеживание
        session.delete(tracking)
        session.commit()
        session.close()
        try:
            log_user_action(session, user_id, 'bot_tracking_stop', f'Доктор {doctor_api_id}', source='bot', status='warning')
        except Exception:
            pass
        await callback.answer("Вы прекратили отслеживание врача.", show_alert=True)
        # Сбрасываем состояние
        await state.clear()
    else:
        # Начинаем отслеживание — создаём запись
        track = UserTrackedDoctor(
            telegram_user_id=user_id,
            doctor_api_id=doctor_api_id,
            tracking_rules=[],
            auto_booking=False
        )
        session.add(track)
        session.commit()
        try:
            log_user_action(session, user_id, 'bot_tracking_start', f'Доктор {doctor_api_id}', source='bot', status='success')
        except Exception:
            pass
        session.close()

        await callback.answer("Вы начали отслеживать изменения. Укажите дни и часы.", show_alert=True)
        await callback.message.answer(
            "Укажите дни/даты и часы. Пример:\n"
            "понедельник: 08:00-12:00, 25 марта: 09:00-11:00, сегодня: 10:00-14:00, завтра: 15:00-17:00\n\n"
            "После ввода жду одним сообщением!"
        )
        # Переходим в состояние ввода дней/часов
        await state.set_state(ProfileStates.waiting_for_tracking_days)


async def track_doctor_days_input_handler(message: Message, state: FSMContext):
    """
    Ловим ввод пользователя:
    "понедельник: 08:00-12:00, 25 марта: 09:00-11:00"
    """
    user_id = message.from_user.id
    session = get_db_session()

    track = session.query(UserTrackedDoctor) \
        .filter_by(telegram_user_id=user_id) \
        .order_by(UserTrackedDoctor.id.desc()) \
        .first()

    if not track or track.tracking_rules:
        # уже заполнено или нет записи
        session.close()
        await state.clear()
        return

    text = message.text.strip()
    rules = parse_user_tracking_input(text)  # функция парсинга
    track.tracking_rules = rules
    session.commit()
    try:
        from database import DoctorInfo as _DocInfoInit
        d_obj = session.query(_DocInfoInit).filter_by(doctor_api_id=str(track.doctor_api_id)).first()
        doc_label = f"{d_obj.name} (ID {track.doctor_api_id})" if d_obj and d_obj.name else track.doctor_api_id
        log_user_action(session, user_id, 'bot_rules_initial', f'Доктор {doc_label}, правил: {len(rules)}', source='bot', status='success')
    except Exception:
        pass

    if track.auto_booking:
        # Если автозапись уже включена, попробуем предложить слоты
        await try_offer_slots_for_track(track, session)
        session.close()
        await message.answer("Правила обновлены. Попытка автозаписи выполнена.")
        await state.clear()
    else:
        session.close()
        # Теперь спрашиваем про автозапись
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Да, автозапись", callback_data="auto_booking_yes"),
                InlineKeyboardButton(text="Нет, только уведомлять", callback_data="auto_booking_no"),
            ]
        ])
        await message.answer("Настроим автозапись?", reply_markup=keyboard)

        # Переходим к состоянию waiting_for_auto_booking
        await state.set_state(ProfileStates.waiting_for_auto_booking)


async def edit_tracking_rules_handler(message: Message, state: FSMContext):
    """Редактирование (добавление или полная замена) правил без их предварительного сброса."""
    data = await state.get_data()
    doctor_api_id = data.get('edit_doctor_id')
    if not doctor_api_id:
        await message.answer("Неизвестный врач для редактирования правил.")
        await state.clear()
        return
    user_id = message.from_user.id
    session = get_db_session()
    try:
        track = session.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_api_id).first()
        if not track:
            await message.answer("Отслеживание для этого врача не найдено.")
            await state.clear()
            return
        text = message.text.strip()
        replace_all = False
        if text.startswith('!'):
            replace_all = True
            text = text[1:].strip()
        new_rules = parse_user_tracking_input(text)
        if not new_rules:
            await message.answer("Не удалось распознать правила. Проверьте формат.")
            return
        if replace_all:
            track.tracking_rules = new_rules
            try:
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(track, 'tracking_rules')
            except Exception:
                pass
            session.commit()
            # Формируем превью первых правил
            preview = []
            for r in new_rules[:3]:
                if isinstance(r, dict):
                    trs = ','.join(r.get('timeRanges', [])[:2]) if r.get('timeRanges') else ''
                    preview.append(f"{r.get('value','')} {trs}".strip())
                else:
                    preview.append(str(r))
            preview_txt = '; '.join(preview)
            await message.answer(f"Правила заменены. Всего: {len(new_rules)}")
            try:
                from database import DoctorInfo as _DocInfoRep
                d_obj2 = session.query(_DocInfoRep).filter_by(doctor_api_id=str(track.doctor_api_id)).first()
                doc_label2 = f"{d_obj2.name} (ID {track.doctor_api_id})" if d_obj2 and d_obj2.name else track.doctor_api_id
                log_user_action(
                    session, user_id, 'bot_rules_replace',
                    f'Доктор {doc_label2}, теперь: {len(new_rules)} | {preview_txt}',
                    source='bot', status='success'
                )
            except Exception:
                pass
        else:
            existing = track.tracking_rules or []
            # Дедуп: представим каждое правило как ключ
            def rule_key(r):
                if isinstance(r, dict):
                    return (r.get('type'), r.get('value'), tuple(sorted(r.get('timeRanges', []))))
                return ('str', str(r))
            existing_keys = {rule_key(r) for r in existing}
            added = 0
            for r in new_rules:
                k = rule_key(r)
                if k not in existing_keys:
                    existing.append(r)
                    existing_keys.add(k)
                    added += 1
            track.tracking_rules = existing
            try:
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(track, 'tracking_rules')
            except Exception:
                pass
            session.commit()
            await message.answer(f"Добавлено новых правил: {added}. Теперь всего: {len(existing)}")
            try:
                action_name = 'bot_rules_add' if added > 0 else 'bot_rules_add_none'
                # Превью только добавленных
                preview_add = []
                for r in new_rules[:3]:
                    if isinstance(r, dict):
                        trs = ','.join(r.get('timeRanges', [])[:2]) if r.get('timeRanges') else ''
                        preview_add.append(f"{r.get('value','')} {trs}".strip())
                    else:
                        preview_add.append(str(r))
                from database import DoctorInfo as _DocInfoAdd
                d_obj3 = session.query(_DocInfoAdd).filter_by(doctor_api_id=str(track.doctor_api_id)).first()
                doc_label3 = f"{d_obj3.name} (ID {track.doctor_api_id})" if d_obj3 and d_obj3.name else track.doctor_api_id
                log_user_action(
                    session, user_id, action_name,
                    f'Dоктор {doc_label3}, добавлено: {added}, всего: {len(existing)} | {"; ".join(preview_add)}',
                    source='bot', status='success' if added>0 else 'info'
                )
            except Exception:
                pass
        # Автозапись попытка
        if track.auto_booking:
            await try_offer_slots_for_track(track, session)
        await state.clear()
    finally:
        session.close()

async def track_auto_booking_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    session = get_db_session()

    track = session.query(UserTrackedDoctor) \
        .filter_by(telegram_user_id=user_id) \
        .order_by(UserTrackedDoctor.id.desc()) \
        .first()

    if not track:
        session.close()
        await callback.answer("Настройка не найдена...", show_alert=True)
        await state.clear()
        return

    user_choice = callback.data
    if user_choice == "auto_booking_yes":
        track.auto_booking = True
        session.commit()
        await try_offer_slots_for_track(track, session)
        session.close()
        await callback.answer("Теперь бот будет автоматически записывать!", show_alert=True)
    else:
        track.auto_booking = False
        session.commit()
        await try_offer_slots_for_track(track, session)
        session.close()
        await callback.answer("Будем только уведомлять без автозаписи.", show_alert=True)

    await state.clear()  # Завершаем настройку


async def change_auto_callback(callback: CallbackQuery):
    doctor_api_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    session = get_db_session()
    tracking = session.query(UserTrackedDoctor).filter_by(
        telegram_user_id=user_id, doctor_api_id=doctor_api_id
    ).first()
    if tracking:
        old_auto = tracking.auto_booking

        tracking.auto_booking = not tracking.auto_booking
        session.commit()
        # Расширенное логирование (техническое + человеко-читаемое)
        try:
            action_name = 'bot_auto_booking_on' if tracking.auto_booking else 'bot_auto_booking_off'
            log_user_action(
                session, user_id, action_name,
                f'Доктор {tracking.doctor_api_id}', source='bot', status='info'
            )
            # Дополнительная запись на русском для наглядности в списке логов
            human_status = 'включена' if tracking.auto_booking else 'выключена'
            human_level = 'success' if tracking.auto_booking else 'warning'
            log_user_action(
                session, user_id, 'Автозапись',
                f'Доктор {tracking.doctor_api_id}: {human_status}', source='bot', status=human_level
            )
        except Exception as e:
            print(f"[change_auto_callback][log_error] {e}")
        if tracking.auto_booking and not old_auto:
            await try_offer_slots_for_track(tracking, session)
        status = "включена" if tracking.auto_booking else "выключена"
        await callback.answer(f"Автозапись {status}", show_alert=True)
    else:
        await callback.answer("Врач не найден в отслеживаемых", show_alert=True)
    session.close()


async def toggle_active_callback(callback: CallbackQuery):
    doctor_api_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    session = get_db_session()
    tracking = session.query(UserTrackedDoctor).filter_by(
        telegram_user_id=user_id, doctor_api_id=doctor_api_id
    ).first()
    if tracking:
        tracking.active = not tracking.active
        session.commit()
        # Расширенное логирование (техническое + человеко-читаемое)
        try:
            action_name = 'bot_tracking_active_on' if tracking.active else 'bot_tracking_active_off'
            log_user_action(
                session, user_id, action_name,
                f'Доктор {tracking.doctor_api_id}', source='bot', status='info'
            )
            human_status = 'возобновлено' if tracking.active else 'приостановлено'
            human_level = 'success' if tracking.active else 'warning'
            log_user_action(
                session, user_id, 'Отслеживание',
                f'Доктор {tracking.doctor_api_id}: {human_status}', source='bot', status=human_level
            )
        except Exception as e:
            print(f"[toggle_active_callback][log_error] {e}")
        status = "возобновлено" if tracking.active else "приостановлено"
        await callback.answer(f"Отслеживание {status}", show_alert=True)
    else:
        await callback.answer("Врач не найден в отслеживаемых", show_alert=True)
    session.close()


async def skip_notification_callback(callback: CallbackQuery):
    await callback.answer("Уведомление пропущено", show_alert=True)


async def change_rules_callback(callback: CallbackQuery, state: FSMContext):
    doctor_api_id = callback.data.split(":")[1]
    user_id = callback.from_user.id
    session = get_db_session()
    tracking = session.query(UserTrackedDoctor).filter_by(
        telegram_user_id=user_id, doctor_api_id=doctor_api_id
    ).first()
    if tracking:
        # Показываем текущие правила и просим прислать новые для добавления
        existing = tracking.tracking_rules or []
        # Форматируем для вывода
        def fmt_rule(r):
            if isinstance(r, dict):
                v = r.get('value', '')
                trs = r.get('timeRanges', [])
                if trs:
                    return "; ".join(f"{v} {tr}" if v else tr for tr in trs)
                return v or '—'
            return str(r)
        existing_text = '\n'.join(f"• {fmt_rule(r)}" for r in existing) if existing else 'нет'
        session.close()
        await callback.answer("Редактирование правил", show_alert=True)
        await callback.message.answer(
            "Текущие правила:\n" + existing_text + "\n\n" +
            "Отправьте новые правила для ДОБАВЛЕНИЯ (формат как раньше).\n" +
            "Чтобы заменить полностью начните сообщение со знака ! (пример: ! понедельник: 08:00-12:00).\n" +
            "Поддерживается ввод: '2025-10-10 16:00-17:00' или '10.10 09:00-11:00' без двоеточия между датой и временем."
        )
        await state.update_data(edit_doctor_id=doctor_api_id)
        await state.set_state(ProfileStates.editing_tracking_rules)
    else:
        await callback.answer("Врач не найден в отслеживаемых", show_alert=True)
        session.close()


def normalize_time(time_str: str) -> str:
    """Нормализует время к HH:MM."""
    time_str = time_str.strip()
    if ':' in time_str:
        parts = time_str.split(':')
        if len(parts) == 2:
            try:
                h = int(parts[0])
                m = int(parts[1])
                return f"{h:02d}:{m:02d}"
            except ValueError:
                pass
    elif len(time_str) == 4 and time_str.isdigit():
        return f"{time_str[:2]}:{time_str[2:]}"
    return time_str


def normalize_time_range(tr: str) -> str:
    """Нормализует строку времени к формату HH:MM-HH:MM."""
    tr = tr.replace(' ', '').replace('—', '-').replace('–', '-')
    parts = tr.split('-')
    if len(parts) == 2:
        start = normalize_time(parts[0])
        end = normalize_time(parts[1])
        return f"{start}-{end}"
    elif len(parts) == 4 and all(len(p) == 2 for p in parts):
        # Для "9-00-10-00" -> "09:00-10:00"
        start = normalize_time(f"{parts[0]}:{parts[1]}")
        end = normalize_time(f"{parts[2]}:{parts[3]}")
        return f"{start}-{end}"
    return tr


def parse_user_tracking_input_legacy(text: str):
    """
    Условная функция, разбивающая строку вида:
    "понедельник: 08:00-12:00, 25 марта: 09:00-11:00"
    или с другими разделителями: "10 октября — 15 — 00-17:00"
    на массив правил:
    [
      {
        "type": "weekday",
        "value": "понедельник",
        "timeRanges": ["08:00-12:00"]
      },
      {
        "type": "date",
        "value": "25 марта",
        "timeRanges": ["09:00-11:00"]
      }
    ]
    """
    if not text:
        return []

    separators = [":"]  # Только двоеточие как разделитель

    parts = [p.strip() for p in text.split(",")]
    rules = []
    for part in parts:
        # Найти первый разделитель
        sep_index = -1
        sep = None
        for s in separators:
            idx = part.find(s)
            if idx != -1 and (sep_index == -1 or idx < sep_index):
                sep_index = idx
                sep = s

        if sep is None:
            # Нет разделителя, просто день
            day_val = part.lower()
            rules.append({
                "type": "weekday" if day_val in ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота",
                                                 "воскресенье"] else "date",
                "value": day_val,
                "timeRanges": []
            })
            continue

        day_part = part[:sep_index].strip()
        time_part = part[sep_index + len(sep):].strip()
        # Нормализуем разделители в времени: заменяем —, -, – на :
        for s in separators:
            time_part = time_part.replace(s, ":")
        day_val = day_part.lower()
        # предполагаем 1 или несколько интервалов, разделённых ";"
        timeRanges = [t.strip() for t in time_part.split(";")]
        rule_type = "weekday" if day_val in ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота",
                                             "воскресенье"] else "date"

        rules.append({
            "type": rule_type,
            "value": day_val,
            "timeRanges": timeRanges
        })

    # Нормализуем правила + ФИКСАЦИЯ относительных значений в момент ввода
    today = datetime.now().date()
    current_wd = datetime.now().weekday()
    for rule in rules:
        rule['value'] = rule['value'].strip().lower()
        rule['timeRanges'] = [normalize_time_range(tr) for tr in rule['timeRanges']]
        # Заморозка только относительных ('сегодня','завтра'), weekday оставляем как повторяющиеся
        if rule['type'] == 'date' and rule['value'] in ('сегодня', 'завтра'):
            target = today if rule['value'] == 'сегодня' else today + timedelta(days=1)
            rule['value'] = target.strftime('%Y-%m-%d')
        elif rule['type'] == 'weekday':
            # ничего не делаем – правило остаётся повторяющимся
            pass
        if rule['type'] == 'date':
            parsed = _parse_date_rule(rule['value'], datetime.now().year)
            if parsed:
                # Приводим к ISO
                rule['value'] = parsed.strftime('%Y-%m-%d')

    return rules


def _freeze_rules_if_needed(track, session):
    """Однократно конвертирует относительные/дни недели правила уже сохранённого трека в абсолютные даты.
    Нужна для старых записей, созданных до введения фиксации при вводе.
    """
    changed = False
    frozen = []
    if not track.tracking_rules:
        return
    today = datetime.now().date()
    current_wd = datetime.now().weekday()
    for r in track.tracking_rules:
        if not isinstance(r, dict):
            frozen.append(r)
            continue
        rtype = (r.get('type') or '').lower()
        value = (r.get('value') or '').strip().lower()
        trs = r.get('timeRanges') or []
        # Заморозка только для 'сегодня'/'завтра' если вдруг они ещё лежат
        if rtype in ('relative_date', 'date') and value in ('сегодня', 'завтра'):
            target = today if value == 'сегодня' else today + timedelta(days=1)
            frozen.append({'type': 'date', 'value': target.strftime('%Y-%m-%d'), 'timeRanges': trs})
            changed = True
        elif rtype == 'weekday':
            # оставляем как есть – повторяющееся правило
            frozen.append({'type': 'weekday', 'value': value, 'timeRanges': trs})
        elif rtype == 'date':
            dt_p = _parse_date_rule(value, datetime.now().year)
            if dt_p and re.match(r'^\d{4}-\d{2}-\d{2}$', value) is None:
                frozen.append({'type': 'date', 'value': dt_p.strftime('%Y-%m-%d'), 'timeRanges': trs})
                changed = True
            else:
                frozen.append(r)
        else:
            frozen.append(r)
    if changed:
        track.tracking_rules = frozen
        try:
            session.commit()
            logging.debug(f"FROZE_RULES user={track.telegram_user_id} doctor={track.doctor_api_id}")
        except Exception as fr_err:
            session.rollback()
            logging.warning(f"FROZE_RULES_COMMIT_FAIL user={track.telegram_user_id} doctor={track.doctor_api_id}: {fr_err}")


def _cleanup_outdated_rules(track, session):
    """Удаляет устаревшие date правила (дата < сегодня)."""
    if not track.tracking_rules:
        return
    today = datetime.now().date()
    cleaned = []
    removed_count = 0
    
    for r in track.tracking_rules:
        if not isinstance(r, dict):
            cleaned.append(r)
            continue
        rtype = (r.get('type') or '').lower()
        value = (r.get('value') or '').strip()
        trs = r.get('timeRanges') or []
        # Заморозка только для 'сегодня'/'завтра' если вдруг они ещё лежат
        if rtype in ('relative_date', 'date') and value in ('сегодня', 'завтра'):
            target = today if value == 'сегодня' else today + timedelta(days=1)
            cleaned.append({'type': 'date', 'value': target.strftime('%Y-%m-%d'), 'timeRanges': trs})
            changed = True
        elif rtype == 'weekday':
            # оставляем как есть – повторяющееся правило
            cleaned.append({'type': 'weekday', 'value': value, 'timeRanges': trs})
        elif rtype == 'date':
            dt_p = _parse_date_rule(value, datetime.now().year)
            if dt_p and re.match(r'^\d{4}-\d{2}-\d{2}$', value) is None:
                cleaned.append({'type': 'date', 'value': dt_p.strftime('%Y-%m-%d'), 'timeRanges': trs})
                changed = True
            else:
                cleaned.append(r)
        else:
            cleaned.append(r)
    if changed:
        track.tracking_rules = cleaned
        try:
            session.commit()
            logging.info(f"CLEANUP_OUTDATED user={track.telegram_user_id} doctor={track.doctor_api_id} removed={removed_count}")
        except Exception as cl_err:
            session.rollback()
            logging.warning(f"CLEANUP_OUTDATED_FAIL user={track.telegram_user_id}: {cl_err}")


async def help_handler(message: Message) -> None:
    await message.answer(
        "Доступные команды:\n"
        "/start — начало работы\n"
        "/auth — ввод токенов\n"
        "/register_profile — регистрация профиля (ОМС и дата рождения)\n"
        "/get_profile_info — получение данных по профилю\n"
        "/whoami — запрос к API whoAmI\n\n"
        "/get_receptions — данные о приёмах\n"
        "/get_referrals — данные о направлениях\n"
        "/get_specialities — информация о специальностях\n"
        "/favourites — расписание любимых врачей\n"
        "/tracked — список отслеживаемых врачей\n"
        "/set_password <пароль> — установить пароль для веб-доступа\n"
        "/get_password — показать текущий пароль\n"
        "/help — помощь"
    )


# Регистрация обработчиков
def register_handlers(dp: Dispatcher) -> None:
    from aiogram.filters import Command
    dp.message.register(start_handler, Command("start"))
    # Команда /clear_schedule отключена по запросу пользователя
    dp.message.register(auth_handler, Command("auth"))
    dp.message.register(help_handler, Command("help"))
    dp.message.register(whoami_handler, Command("whoami"))
    dp.message.register(get_profile_info_handler, Command("get_profile_info"))
    dp.message.register(register_profile_handler, Command("register_profile"))
    dp.message.register(get_profile_info_handler, Command("get_profile_info"))
    dp.message.register(get_receptions_handler, Command("get_receptions"))
    dp.message.register(get_referrals_handler, Command("get_referrals"))
    dp.message.register(get_specialities_handler, Command("get_specialities"))
    dp.message.register(get_doctors_info_handler, Command("get_doctors_info"))
    dp.message.register(get_clinics_handler, Command("get_clinics"))
    dp.message.register(ldp_aggregate_handler, Command("ldp_agg"))
    dp.message.register(favourites_handler, Command("favourites"))
    dp.message.register(set_password_handler, Command("set_password"))
    dp.message.register(get_password_handler, Command("get_password"))
    dp.message.register(access_token_handler, AuthStates.waiting_for_access_token)
    dp.message.register(refresh_token_handler, AuthStates.waiting_for_refresh_token)
    dp.message.register(oms_number_handler, ProfileStates.waiting_for_oms_number)
    dp.message.register(birth_date_handler, ProfileStates.waiting_for_birth_date)
    dp.message.register(track_doctor_days_input_handler, ProfileStates.waiting_for_tracking_days)
    dp.message.register(edit_tracking_rules_handler, ProfileStates.editing_tracking_rules)
    dp.callback_query.register(track_auto_booking_callback, F.data.in_(["auto_booking_yes", "auto_booking_no"]), ProfileStates.waiting_for_auto_booking)
    dp.message.register(tracked_handler, Command("tracked"))
    # Регистрируем callback-хэндлер для переключения избранного
    dp.callback_query.register(toggle_favorite_callback_handler, F.data.startswith("fav_toggle:"))
    dp.callback_query.register(track_schedule_toggle_callback_handler, F.data.startswith("track_toggle:"))
    dp.callback_query.register(process_reschedule, F.data.startswith("reschedule:"))
    dp.callback_query.register(do_reschedule_callback, F.data.startswith("do_reschedule:"))
    dp.callback_query.register(change_auto_callback, F.data.startswith("change_auto:"))
    dp.callback_query.register(change_rules_callback, F.data.startswith("change_rules:"))
    dp.callback_query.register(book_slot_callback, F.data.startswith("book_slot:"))
    dp.callback_query.register(toggle_active_callback, F.data.startswith("toggle_active:"))
    dp.callback_query.register(skip_notification_callback, F.data.startswith("skip_notification:"))


import asyncio
from aiogram import Bot, Dispatcher
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from database import get_db_session, UserTrackedDoctor, DoctorInfo, DoctorSchedule, UserDoctorLink
from emias_api import get_available_resource_schedule_info
from aiogram.types import Message
from config import TELEGRAM_BOT_TOKEN

# Создаем бота и диспетчер
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

import json


async def get_schedule_for_doctor(session, user_id: int, doctor: DoctorInfo):
    """
    Получает расписание для врача, пробуя разные appointment_id.
    1. Обновляем актуальные записи из API.
    2. Пробуем для специальностей 602, 69, если основная специальность врача одна из них.
    3. Пробуем для основной специальности врача.
    4. Пробуем без appointment_id, если нет записи.
    """
    if not doctor.complex_resource_id:
        return None

    # Обновляем актуальные записи из API
    appointments_data = get_appointment_receptions_by_patient(user_id)
    if appointments_data:
        appointments = appointments_data.get("appointment", [])
        existing_specs = set()
        for appt in appointments:
            appt_spec_id = extract_speciality_id_from_appointment(appt)
            if appt_spec_id:
                existing_specs.add(appt_spec_id)
            appointment_id = appt.get("appointmentId") or appt.get("id")
            if appointment_id and appt_spec_id:
                link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=appt_spec_id).first()
                if link:
                    link.appointment_id = str(appointment_id)
                    # Сохраняем referral_id если есть
                    ref = appt.get("referral") or {}
                    ref_id = ref.get("referralId") or ref.get("id")
                    if ref_id:
                        link.referral_id = str(ref_id)
                else:
                    # Создаем новую связь (в частности для LDP 600034 и т.п.)
                    link = UserDoctorLink(
                        telegram_user_id=user_id,
                        doctor_speciality=str(appt_spec_id),
                        appointment_id=str(appointment_id),
                    )
                    ref = appt.get("referral") or {}
                    ref_id = ref.get("referralId") or ref.get("id")
                    if ref_id:
                        link.referral_id = str(ref_id)
                    session.add(link)
        # Очищаем appointment_id для specs без активных записей
        all_links = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id).all()
        for link in all_links:
            if link.doctor_speciality not in existing_specs:
                link.appointment_id = None
        session.commit()

    speciality_priorities = []
    # logging.info(f"Получаем расписание для врача: {doctor.name} (ID: {doctor.doctor_api_id}), специальность: {doctor.ar_speciality_id}")
    if doctor.ar_speciality_id in ["602", "69"]:
        speciality_priorities.extend(["602", "69"])

    # Добавляем основную специальность врача в список, если ее там еще нет
    if doctor.ar_speciality_id not in speciality_priorities:
        speciality_priorities.append(doctor.ar_speciality_id)

    # logging.info(f"speciality_priorities: {speciality_priorities}")

    # Проверяем, есть ли appointment_id из API для этого врача или эквивалентных специальностей
    appointment_id = None
    if appointments_data:
        appointments = appointments_data.get("appointment", [])
        for appt in appointments:
            # Сначала проверяем, есть ли запись к этому конкретному врачу
            if str(appt.get("availableResourceId", "")) == str(doctor.doctor_api_id):
                appt_id = appt.get("appointmentId") or appt.get("id")
                if appt_id:
                    try:
                        appointment_id = int(appt_id)
                        print(f"Найден appointment_id {appointment_id} для врача {doctor.doctor_api_id}")
                        break
                    except (ValueError, TypeError):
                        pass
            # Если не нашли для этого врача, проверяем по специальности
            if appointment_id is None:
                appt_spec_id = extract_speciality_id_from_appointment(appt)
                # print(f"Проверяем запись с specialityId: {appt_spec_id}, appointmentId: {appt.get('appointmentId') or appt.get('id')}")
                if appt_spec_id in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                    appt_id = appt.get("appointmentId") or appt.get("id")
                    if appt_id:
                        try:
                            appointment_id = int(appt_id)
                            print(f"Найден appointment_id {appointment_id} по специальности {appt_spec_id}")
                            break
                        except (ValueError, TypeError):
                            pass
    
    logging.info(f"appointment_id found: {appointment_id}")
    print(f"Используем appointment_id: {appointment_id} для специальности {doctor.ar_speciality_id}")
    if appointment_id:
        # Пытаемся с appointment_id, но если пусто или ошибка – пробуем без и очищаем устаревший appointment_id
        need_clear_links = False
        try:
            schedule_response = get_available_resource_schedule_info(
                user_id, available_resource_id=doctor.doctor_api_id, complex_resource_id=doctor.complex_resource_id, appointment_id=appointment_id
            )
            if schedule_response and schedule_response.get("payload") and schedule_response.get("payload").get("scheduleOfDay"):
                return schedule_response
            else:
                logging.info("Переходим к запросу без appointment_id (пустой payload или ошибка)")
                need_clear_links = True
        except Exception as e:
            logging.error(f"Ошибка при запросе с appointment_id {appointment_id}: {e}. Пробуем без appointment_id")
            need_clear_links = True
        # Очищаем устаревший appointment_id в связях пользователя
        if need_clear_links:
            try:
                eq_codes = set(get_equivalent_speciality_codes(doctor.ar_speciality_id)) if doctor.ar_speciality_id else set()
                if doctor.ar_speciality_id:
                    eq_codes.add(doctor.ar_speciality_id)
                links_to_clear = session.query(UserDoctorLink).filter(
                    UserDoctorLink.telegram_user_id == user_id,
                    UserDoctorLink.doctor_speciality.in_(eq_codes)
                ).all()
                cleared = 0
                for l in links_to_clear:
                    if l.appointment_id:
                        l.appointment_id = None
                        cleared += 1
                if cleared:
                    session.commit()
                    logging.info(f"Сброшено устаревших appointment_id: {cleared} для user {user_id}")
            except Exception as ce:
                logging.warning(f"Не удалось очистить устаревшие appointment_id: {ce}")
        # fallback: без appointment_id
        try:
            fallback_response = get_available_resource_schedule_info(
                user_id, available_resource_id=doctor.doctor_api_id, complex_resource_id=doctor.complex_resource_id
            )
            return fallback_response
        except Exception as e2:
            logging.error(f"Ошибка при запросе без appointment_id после ошибки с appointment_id: {e2}")
            return None

    # Если нет appointment_id, пробуем без
    return get_available_resource_schedule_info(
        user_id, doctor.doctor_api_id, doctor.complex_resource_id
    )


async def check_schedule_updates():
    """
    Проверяет изменения в расписании для всех отслеживаемых врачей (UserTrackedDoctor).
    Если изменения обнаружены, отправляет сообщение пользователю.
    Если включён режим авто-записи, пытается записаться на подходящий слот аналогично скриптам blood.py/shift.
    """
    logging.info("Starting check_schedule_updates")
    session = get_db_session()
    tracked_doctors = session.query(UserTrackedDoctor).all()

    if not tracked_doctors:
        session.close()
        logging.info("No tracked doctors")
        return  # Никто ничего не отслеживает

    tracks_to_delete = []
    tracks_to_disable_auto = []

    for track in tracked_doctors:
        user_id = track.telegram_user_id
        doctor = session.query(DoctorInfo).filter_by(doctor_api_id=track.doctor_api_id).first()
        if not doctor:
            tracks_to_delete.append(track)  # Врач не найден, удаляем отслеживание
            continue

        if not track.active:
            continue  # Отслеживание приостановлено

        # Однократно фиксируем относительные/weekday правила в абсолютные даты (для старых треков)
        try:
            _freeze_rules_if_needed(track, session)
            _cleanup_outdated_rules(track, session)
        except Exception as fr_ex:
            logging.debug(f"Freeze rules skipped (non-critical) doctor={track.doctor_api_id}: {fr_ex}")

        # СТАРОЕ РАСПИСАНИЕ ДОЛЖНО БЫТЬ СЧИТАНО ДО ЗАПРОСА НОВОГО
        # (по требованию: schedule_response внутри get_schedule_for_doctor может опосредованно влиять на состояние)
        old_schedule_record = session.query(DoctorSchedule).filter_by(
            doctor_api_id=track.doctor_api_id
        ).first()
        baseline_missing = old_schedule_record is None
        old_schedule_json_raw = old_schedule_record.schedule_text if old_schedule_record else None
        if old_schedule_json_raw is not None:
            try:
                old_data = json.loads(old_schedule_json_raw)
                # logging.debug(
                #     "BASELINE_CAPTURE %s: bytes=%d days=%d baseline_missing=%s", 
                #     doctor.name,
                #     len(old_schedule_json_raw.encode('utf-8')),
                #     len(old_data) if isinstance(old_data, list) else -1,
                #     baseline_missing
                # )
            except Exception as cap_err:
                # logging.warning(f"BASELINE_CAPTURE_PARSE_FAIL {doctor.name}: {cap_err}; treating as empty old data")
                old_data = []
        else:
            old_data = []
        schedule_response = await get_schedule_for_doctor(session, user_id, doctor)

        if not schedule_response or not schedule_response.get("payload"):
            continue

        new_schedule = schedule_response.get("payload").get("scheduleOfDay") or []
        new_schedule_json = json.dumps(new_schedule, ensure_ascii=False)

        normalized_rules = _normalize_rules(track.tracking_rules)
        matching_slots = collect_matching_slots(schedule_response.get("payload"), normalized_rules)
        best_slot_info = matching_slots[0] if matching_slots else None
        best_slot_display = best_slot_info[0] if best_slot_info else None

        # Если ничего не подобрали, но в новом расписании есть слоты — диагностируем почему
        if not matching_slots and new_schedule:
            try:
                # Соберём ВСЕ raw слоты
                raw_slots_full = parse_schedule_payload(new_schedule)
                future_raw = []
                now_local_diag = datetime.now().replace(second=0, microsecond=0)
                for s in sorted(raw_slots_full):
                    try:
                        dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
                        if dt > now_local_diag:
                            future_raw.append(dt)
                    except ValueError:
                        continue
                future_raw = future_raw[:25]  # ограничим диагностический объём

                # Подробный разбор первой пачки слотов по правилам
                def _analyze(dt: datetime, rules: List[Dict[str, Any]]):
                    reasons = []
                    if not rules:
                        return {"match": True, "reasons": ["no_rules -> always_true"]}
                    slot_date = dt.date()
                    slot_time_obj = dt.time().replace(second=0, microsecond=0)
                    for idx, rule in enumerate(rules):
                        rtype = (rule.get('type') or '').lower()
                        val = (rule.get('value') or '').strip().lower()
                        trs = rule.get('timeRanges') or []
                        rule_ok = False
                        detail = {"rule_index": idx, "type": rtype, "value": val, "timeRanges": trs}
                        if rtype == 'weekday':
                            wd = WEEKDAY_NAME_TO_INDEX.get(val)
                            if wd is not None and dt.weekday() == wd:
                                time_ok = _time_matches_ranges(slot_time_obj, trs)
                                rule_ok = time_ok
                                detail["weekday_ok"] = True
                                detail["time_ok"] = time_ok
                            else:
                                detail["weekday_ok"] = False
                        elif rtype == 'date':
                            td = _parse_date_rule(val, datetime.now().year)
                            if td == slot_date:
                                time_ok = _time_matches_ranges(slot_time_obj, trs)
                                rule_ok = time_ok
                                detail["date_ok"] = True
                                detail["time_ok"] = time_ok
                            else:
                                detail["date_ok"] = False
                        elif rtype == 'relative_date':
                            if val == 'сегодня':
                                target = date.today()
                            elif val == 'завтра':
                                target = date.today() + timedelta(days=1)
                            else:
                                target = None
                            if target and target == slot_date:
                                time_ok = _time_matches_ranges(slot_time_obj, trs)
                                rule_ok = time_ok
                                detail["rel_date_ok"] = True
                                detail["time_ok"] = time_ok
                            else:
                                detail["rel_date_ok"] = False
                        elif rtype == 'any':
                            time_ok = _time_matches_ranges(slot_time_obj, trs)
                            rule_ok = time_ok
                            detail["time_ok"] = time_ok
                        detail["rule_match"] = rule_ok
                        reasons.append(detail)
                        if rule_ok:
                            # Достаточно одного правила
                            return {"match": True, "reasons": reasons}
                    return {"match": False, "reasons": reasons}

            except Exception as diag_err:
                logging.warning(f"NO_MATCH_DIAG_ERROR {doctor.name}: {diag_err}")

        # Старое расписание уже считано выше (old_schedule_record / baseline_missing)

        # ===================== AUTO-BOOKING BRANCH =====================
        # Проблема (наблюдалась): пока включена автозапись, мы ранее НЕ обновляли baseline,
        # поэтому когда автозапись выключалась (успешная запись) – следующий цикл видел «старый» снапшот
        # и считал ВСЕ текущие слоты added. Теперь даже в режиме auto_booking мы обновляем baseline
        # (без вычисления diff и без уведомлений) чтобы состояние было консистентным.
        if track.auto_booking:
            try:
                if baseline_missing:
                    old_schedule_record = DoctorSchedule(doctor_api_id=doctor.doctor_api_id, schedule_text=new_schedule_json)
                    session.add(old_schedule_record)
                    # logging.debug(f"[AUTO_BOOK] Baseline created for {doctor.name} before booking attempt")
                else:
                    old_schedule_record.schedule_text = new_schedule_json
                    # logging.debug(f"[AUTO_BOOK] Baseline updated for {doctor.name} before booking attempt")
                session.commit()
            except Exception as bl_err:
                session.rollback()
                logging.warning(f"[AUTO_BOOK] Failed to sync baseline for {doctor.name}: {bl_err}")

            if best_slot_display:
                # logging.info(f"Auto-book INIT {doctor.name}: trying slot={best_slot_display}")
                success, result_kind = await book_appointment(user_id, doctor.doctor_api_id, best_slot_display)
                logging.info(f"Auto-book RESULT {doctor.name}: slot={best_slot_display} success={success} kind={result_kind}")
                # Уведим пользователя и при успехе выключим автозапись (одноразовая логика)
                if success:
                    if result_kind == "shift":
                        action = 'auto_book_shift'
                        note_body = "Приём перенесён"
                    else:
                        action = 'auto_book_success'
                        note_body = "Запись создана"
                    note = (
                        f"✅ <b>{note_body}</b>\n"
                        f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})\n"
                        f"Слот: {best_slot_display}\n"
                        f"Автозапись отключена."
                    )
                    track.auto_booking = False
                    try:
                        log_user_action(session, user_id, action, f"doctor={doctor.doctor_api_id} slot={best_slot_display}", source='bot', status='success')
                    except Exception:
                        pass
                else:
                    action = 'auto_book_fail'
                    note = (
                        f"⚠️ <b>Автозапись не удалась</b>\n"
                        f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})\n"
                        f"Слот: {best_slot_display}\n"
                        f"Ошибка: {safe_html(result_kind) if result_kind else 'Неизвестная ошибка'}"
                    )
                    try:
                        log_user_action(session, user_id, action, f"doctor={doctor.doctor_api_id} slot={best_slot_display} err={result_kind}", source='bot', status='error')
                    except Exception:
                        pass
                # Отправка пользователю (всегда пробуем, даже при ошибке логирования)
                try:
                    await bot.send_message(user_id, safe_html(note), parse_mode="HTML")
                except Exception as send_err:
                    logging.warning(f"Failed to send auto-book notification to user {user_id}: {send_err}")
            # Сохраняем возможное отключение автозаписи
            try:
                session.commit()
            except Exception as commit_err:
                session.rollback()
                # logging.warning(f"[AUTO_BOOK] Commit error after booking attempt for {doctor.name}: {commit_err}")
            continue  # переходим к следующему отслеживанию

        # Больше НЕ перечитываем baseline (чтобы не изменился между захватом и diff)
        if baseline_missing:
            logging.info(f"Baseline missing for {doctor.name} (first seen this run)")

        # Сравнение
        added, removed, changes_text = compare_schedules_payloads(old_data, new_schedule)
        # После сравнения обновляем или создаём запись расписания
        if baseline_missing:
            old_schedule_record = DoctorSchedule(doctor_api_id=doctor.doctor_api_id, schedule_text=new_schedule_json)
            session.add(old_schedule_record)
        else:
            old_schedule_record.schedule_text = new_schedule_json
        session.commit()

        # Даже если нет текстовых изменений (changes_text пуст), всё равно проверяем слоты по правилам
        relevant_added = filter_slots_by_rules(added, normalized_rules)
        relevant_removed = filter_slots_by_rules(removed, normalized_rules)
        # Все текущие подходящие слоты (могут быть те же, что и раньше)
        all_current_slots = parse_schedule_payload(
            schedule_response.get("payload").get("scheduleOfDay") or []
        )
        all_relevant_now = filter_slots_by_rules(all_current_slots, normalized_rules)
        
        # # Информативный лог только при изменениях или активности
        # if relevant_added or relevant_removed or (not all_relevant_now and matching_slots):
        #     logging.info(
        #         "Relevant for %s: new_added=%d, new_removed=%d, total_now=%d, best_slot=%s",
        #         doctor.name,
        #         len(relevant_added),
        #         len(relevant_removed),
        #         len(all_relevant_now),
        #         best_slot_display or "-"
        #     )

        # Определяем сколько релевантных слотов было раньше, чтобы поймать сценарий "было 0 стало N" без diff added
        try:
            old_slots_all = parse_schedule_payload(old_data) if old_data else set()
        except Exception:
            old_slots_all = set()
        old_relevant_before = filter_slots_by_rules(old_slots_all, normalized_rules)
        old_relevant_count = len(old_relevant_before)

        initial_reveal = False
        if (old_relevant_count == 0 and len(all_relevant_now) > 0 and not relevant_added) or (baseline_missing and all_relevant_now):
            initial_reveal = True

        # DEBUG: логируем статус специально отслеживаемых слотов
        if DEBUG_SLOTS:
            try:
                # old_slots_all уже вычислен выше; all_current_slots / all_relevant_now тоже есть
                new_all_slots = parse_schedule_payload(new_schedule) if new_schedule else set()
                added_set = added if isinstance(added, set) else set(added)
                relevant_added_set = set(relevant_added)
                all_relevant_now_set = set(all_relevant_now)
                # for dbg_slot in DEBUG_SLOTS:
                #     logging.info(
                #         "DEBUG_SLOT doctor=%s slot=%s old_present=%s new_present=%s in_added=%s in_relevant_added=%s in_all_relevant_now=%s initial_reveal=%s",
                #         doctor.name,
                #         dbg_slot,
                #         dbg_slot in (old_slots_all if 'old_slots_all' in locals() else set()),
                #         dbg_slot in new_all_slots,
                #         dbg_slot in added_set,
                #         dbg_slot in relevant_added_set,
                #         dbg_slot in all_relevant_now_set,
                #         initial_reveal
                #     )
            except Exception as dbg_e:
                logging.warning(f"DEBUG_SLOT logging error: {dbg_e}")
    # Ручной режим (auto_booking = False):
    # Требование: уведомлять только при появлении новых релевантных слотов или при первом появлении вообще.
        if not track.auto_booking:
            have_relevant_now = bool(all_relevant_now)
            # Условие: либо initial_reveal (раньше было 0), либо есть новые релевантные (relevant_added)
            if initial_reveal or relevant_added:
                new_schedule_text = format_schedule_message_simple(schedule_response.get("payload"))
                msg_parts = [
                    ("📢 <b>Появились подходящие слоты!</b>" if initial_reveal else "📢 <b>Новые подходящие слоты!</b>"),
                    f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})"
                ]
                # Показываем только новые релевантные слоты (или все, если initial_reveal)
                slots_for_keyboard = all_relevant_now if initial_reveal else relevant_added
                if slots_for_keyboard:
                    msg_parts.extend([
                        "",
                        "🎯 <b>Доступно:</b>",
                        group_slots_by_date(set(slots_for_keyboard))
                    ])
                if best_slot_display and have_relevant_now:
                    msg_parts.extend(["", f"🔎 Ближайший: {best_slot_display}"])
                msg_parts.extend(["", f"📅 {new_schedule_text}"])

                msg = "\n".join(p for p in msg_parts if p is not None)

                keyboard = None
                if slots_for_keyboard:
                    MAX_BTNS = 30
                    sorted_slots = sorted(slots_for_keyboard)[:MAX_BTNS]
                    buttons = []
                    row = []
                    for slot in sorted_slots:
                        display_time = slot.split()[1] if ' ' in slot else slot
                        row.append(InlineKeyboardButton(text=display_time, callback_data=f"book_slot:{doctor.doctor_api_id}:{slot}"))
                        if len(row) == 3:
                            buttons.append(row)
                            row = []
                    if row:
                        buttons.append(row)
                    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

                if len(msg) > 4000:
                    parts = [msg[i:i+4000] for i in range(0, len(msg), 4000)]
                    for i, part in enumerate(parts):
                        reply_markup = keyboard if i == len(parts) - 1 else None
                        await bot.send_message(user_id, safe_html(part), parse_mode="HTML", reply_markup=reply_markup)
                else:
                    await bot.send_message(user_id, safe_html(msg), parse_mode="HTML", reply_markup=keyboard)
            # Переходим к следующему треку
            continue

    for t in tracks_to_delete:
        session.delete(t)
    for t in tracks_to_disable_auto:
        t.auto_booking = False
    session.commit()
    session.close()
    logging.info("Finished check_schedule_updates")


async def try_offer_slots_for_track(track: UserTrackedDoctor, session):
    """
    Находит подходящие слоты для данного отслеживания и предлагает пользователю выбрать для записи.
    """
    user_id = track.telegram_user_id
    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=track.doctor_api_id).first()
    if not doctor:
        return

    schedule_response = await get_schedule_for_doctor(session, user_id, doctor)
    if not schedule_response or not schedule_response.get("payload"):
        return

    new_schedule = schedule_response.get("payload").get("scheduleOfDay") or []
    new_slots = parse_schedule_payload(new_schedule)
    normalized_rules = _normalize_rules(track.tracking_rules)
    logging.info(f"Rules (normalized): {normalized_rules}")
    logging.info(f"New slots count: {len(new_slots)}")
    relevant_slots = filter_slots_by_rules(new_slots, normalized_rules)
    logging.info(f"Relevant slots: {relevant_slots}")

    if not relevant_slots:
        # Формируем строку с правилами отдельно
        rules_text = ', '.join([f"{r.get('value')} {','.join(r.get('timeRanges') or [])}" for r in normalized_rules])
        note = (
            f"ℹ️ <b>Подходящих слотов нет</b>\n"
            f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})\n"
            f"Правила: {rules_text}"
        )
        from database import log_user_action
        log_user_action(session, user_id, 'slots_not_found', f'Доктор {doctor.doctor_api_id} правила: {rules_text}', source='bot', status='error')
        await bot.send_message(user_id, safe_html(note), parse_mode="HTML")
        return

    # Предложить слоты
    msg_parts = [
        f"🎯 <b>Найдены подходящие слоты</b>",
        f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})",
        "",
        "Выберите слот для записи:"
    ]

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for slot in sorted(relevant_slots):
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=slot, callback_data=f"book_slot:{doctor.doctor_api_id}:{slot}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⏭️ Пропустить", callback_data=f"skip_notification:{doctor.doctor_api_id}")])

    msg = "\n".join(part for part in msg_parts if part is not None)

    from database import log_user_action
    log_user_action(session, user_id, 'slots_found', f'Доктор {doctor.doctor_api_id} слотов: {len(relevant_slots)}', source='bot', status='success')
    await bot.send_message(user_id, safe_html(msg), parse_mode="HTML", reply_markup=keyboard)


async def send_slot_selection_message(track: UserTrackedDoctor, session):
    """
    Отправляет сообщение с подходящими слотами и кнопками для выбора записи.
    """
    user_id = track.telegram_user_id
    doctor = session.query(DoctorInfo).filter_by(doctor_api_id=track.doctor_api_id).first()
    if not doctor:
        return

    schedule_response = await get_schedule_for_doctor(session, user_id, doctor)
    if not schedule_response or not schedule_response.get("payload"):
        return

    new_schedule = schedule_response.get("payload").get("scheduleOfDay") or []
    new_schedule_json = json.dumps(new_schedule, ensure_ascii=False)
    # Обновим расписание
    old_schedule_record = session.query(DoctorSchedule).filter_by(
        doctor_api_id=doctor.doctor_api_id
    ).first()
    if not old_schedule_record:
        session.add(DoctorSchedule(
            doctor_api_id=doctor.doctor_api_id,
            schedule_text=new_schedule_json
        ))
    else:
        old_schedule_record.schedule_text = new_schedule_json
    session.commit()

    normalized_rules = _normalize_rules(track.tracking_rules)
    relevant_added = filter_slots_by_rules(parse_schedule_payload(new_schedule), normalized_rules)
    if not relevant_added:
        return

    new_schedule_text = format_schedule_message_simple(schedule_response.get("payload"))

    msg_parts = [
        f"ℹ️ <b>Подходящие слоты для записи</b>\n"
        f"👨‍⚕️ {doctor.name} ({doctor.ar_speciality_name})\n",
        "",
        f"📅 {new_schedule_text}",
        "",
        "🎯 <b>Выберите слот для записи:</b>"
    ]

    msg = "\n".join(part for part in msg_parts if part is not None)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for slot in sorted(relevant_added):
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=slot, callback_data=f"book_slot:{doctor.doctor_api_id}:{slot}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="⏭️ Пропустить", callback_data=f"skip_notification:{doctor.doctor_api_id}")])

    await bot.send_message(user_id, safe_html(msg), parse_mode="HTML", reply_markup=keyboard)


from typing import List, Dict, Tuple, Set, Optional, Any
from collections import defaultdict
import re

# Русские месяцы для отображения дат
MONTHS = {
    '01': 'января', '02': 'февраля', '03': 'марта', '04': 'апреля',
    '05': 'мая', '06': 'июня', '07': 'июля', '08': 'августа',
    '09': 'сентября', '10': 'октября', '11': 'ноября', '12': 'декабря',
}

MONTH_NAME_TO_NUM = {
    'январь': 1, 'января': 1,
    'февраль': 2, 'февраля': 2,
    'март': 3, 'марта': 3,
    'апрель': 4, 'апреля': 4,
    'май': 5, 'мая': 5,
    'июнь': 6, 'июня': 6,
    'июль': 7, 'июля': 7,
    'август': 8, 'августа': 8,
    'сентябрь': 9, 'сентября': 9,
    'октябрь': 10, 'октября': 10,
    'ноябрь': 11, 'ноября': 11,
    'декабрь': 12, 'декабря': 12,
}

WEEKDAY_NAME_TO_INDEX = {
    'понедельник': 0,
    'вторник': 1,
    'среда': 2,
    'четверг': 3,
    'пятница': 4,
    'суббота': 5,
    'воскресенье': 6,
}


def _parse_time_range(range_str: str) -> Optional[Tuple[time, time]]:
    if not range_str:
        return None
    cleaned = range_str.strip().replace('.', ':')
    parts = cleaned.split('-', maxsplit=1)
    if len(parts) == 2:
        try:
            start = time.fromisoformat(parts[0].strip())
            end = time.fromisoformat(parts[1].strip())
            return start, end
        except ValueError:
            return None
    # Try to parse as HH:MM:HH:MM
    colon_parts = cleaned.split(':')
    if len(colon_parts) == 4:
        try:
            start = time(int(colon_parts[0]), int(colon_parts[1]))
            end = time(int(colon_parts[2]), int(colon_parts[3]))
            return start, end
        except (ValueError, IndexError):
            pass
    # Fallback to single time
    try:
        point = time.fromisoformat(cleaned)
        return point, point
    except ValueError:
        return None


def _time_matches_ranges(slot_time: time, ranges: Optional[List[str]]) -> bool:
    if not ranges:
        return True
    for range_str in ranges:
        parsed = _parse_time_range(range_str)
        if not parsed:
            continue
        start, end = parsed
        if start <= end:
            if start <= slot_time <= end:
                return True
        else:  # интервал «через полночь»
            if slot_time >= start or slot_time <= end:
                return True
    return False


def _parse_date_rule(value: str, reference_year: int) -> Optional[date]:
    if not value:
        return None
    v = value.strip().lower()
    if v in ("сегодня",):
        return datetime.now().date()
    if v in ("завтра",):
        return datetime.now().date() + timedelta(days=1)

    # ISO форматы
    for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue

    # Форматы без года
    if v.count('.') >= 1:
        parts = [p for p in v.split('.') if p]
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else reference_year
            try:
                return date(year, month, day)
            except ValueError:
                pass
    tokens = v.replace('-', ' ').split()
    if tokens and tokens[0].isdigit():
        day = int(tokens[0])
        month = MONTH_NAME_TO_NUM.get(tokens[1]) if len(tokens) >= 2 else None
        year = None
        if len(tokens) >= 3 and tokens[2].isdigit():
            year = int(tokens[2])
        if month:
            try:
                return date(year or reference_year, month, day)
            except ValueError:
                pass
    return None


def _parse_string_rule(raw: str) -> Optional[Dict[str, Any]]:
    """Преобразует старый строковый формат правила (например: 'завтра 11:20-19:00' или 'понедельник 08:00-12:00')
    в структурированный dict: {type,value,timeRanges}.
    Поддерживаемые префиксы: дни недели, 'сегодня', 'завтра', ISO-дата YYYY-MM-DD, дата в формате DD.MM.YYYY.
    Если префикс не распознан, считаем тип any (ограничение только по времени).
    """
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    # Найти диапазон времени через дефис
    time_match = re.search(r'(\d{1,2}[:.]\d{2})\s?-\s?(\d{1,2}[:.]\d{2})', s)
    time_ranges: List[str] = []
    if time_match:
        tr = f"{time_match.group(1).replace('.',':')}-{time_match.group(2).replace('.',':')}"
        time_ranges.append(tr)
        prefix_part = s[:time_match.start()].strip()
    else:
        prefix_part = s
    prefix_lower = prefix_part.lower()
    rule_type = 'any'
    value = ''
    # Weekday
    if prefix_lower in WEEKDAY_NAME_TO_INDEX:
        rule_type = 'weekday'
        value = prefix_lower
    elif prefix_lower in ('сегодня', 'завтра'):
        rule_type = 'relative_date'
        value = prefix_lower
    else:
        # ISO date
        iso_date = None
        for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
            try:
                dt = datetime.strptime(prefix_lower, fmt)
                iso_date = dt.strftime('%Y-%m-%d')
                break
            except ValueError:
                continue
        if iso_date:
            rule_type = 'date'
            value = iso_date
        else:
            # Попытка DD.MM или DD месяц (рус.) без года
            if re.match(r'^\d{1,2}\.\d{1,2}$', prefix_lower):
                try:
                    day, month = prefix_lower.split('.')
                    dt = date(datetime.now().year, int(month), int(day))
                    value = dt.strftime('%Y-%m-%d')
                    rule_type = 'date'
                except ValueError:
                    pass
            else:
                tokens = prefix_lower.split()
                if tokens and tokens[0].isdigit() and len(tokens) >= 2 and tokens[1] in MONTH_NAME_TO_NUM:
                    try:
                        d = int(tokens[0])
                        m = MONTH_NAME_TO_NUM[tokens[1]]
                        dt = date(datetime.now().year, m, d)
                        value = dt.strftime('%Y-%m-%d')
                        rule_type = 'date'
                    except ValueError:
                        pass
    return {
        'type': rule_type,
        'value': value,
        'timeRanges': time_ranges
    }


def _normalize_rules(rules: Optional[List[Any]]) -> List[Dict[str, Any]]:
    """Принимает список правил в новом (dict) или старом (str) формате и возвращает список dict."""
    if not rules:
        return []
    normalized: List[Dict[str, Any]] = []
    for r in rules:
        if isinstance(r, dict):
            # Гарантируем поля
            normalized.append({
                'type': (r.get('type') or '').lower() or 'any',
                'value': r.get('value') or '',
                'timeRanges': r.get('timeRanges') or []
            })
        elif isinstance(r, str):
            parsed = _parse_string_rule(r)
            if parsed:
                normalized.append(parsed)
    return normalized


def slot_matches_tracking_rules(slot_dt: datetime, rules: Optional[List[Dict[str, Any]]]) -> bool:
    if not rules:
        return True

    slot_date = slot_dt.date()
    slot_time_obj = slot_dt.timetz() if hasattr(slot_dt, "timetz") else slot_dt.time()
    slot_time = slot_time_obj.replace(second=0, microsecond=0, tzinfo=None)

    for rule in rules:
        rule_type = (rule.get("type") or "").lower()
        value = (rule.get("value") or "").strip().lower()
        time_ranges = rule.get("timeRanges") or []

        if rule_type == "weekday":
            weekday_idx = WEEKDAY_NAME_TO_INDEX.get(value)
            if weekday_idx is None or slot_dt.weekday() != weekday_idx:
                continue
            if _time_matches_ranges(slot_time, time_ranges):
                return True
        elif rule_type == "date":
            target_date = _parse_date_rule(value, datetime.now().year)
            if not target_date or target_date != slot_date:
                continue
            if _time_matches_ranges(slot_time, time_ranges):
                return True
        elif rule_type == "relative_date":
            if value == "сегодня":
                target_date = date.today()
            elif value == "завтра":
                target_date = date.today() + timedelta(days=1)
            else:
                continue
            if target_date != slot_date:
                continue
            if _time_matches_ranges(slot_time, time_ranges):
                return True
        elif rule_type == "any":
            if _time_matches_ranges(slot_time, time_ranges):
                return True

    return False


def filter_slots_by_rules(slots: Optional[Set[str]], rules: Optional[List[Dict[str, Any]]]) -> List[str]:
    if not slots:
        return []
    filtered: List[str] = []
    now_local = datetime.now().replace(tzinfo=None)
    for slot in sorted(slots):
        try:
            slot_dt = datetime.strptime(slot, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        if slot_dt <= now_local:
            continue
        if slot_matches_tracking_rules(slot_dt, rules):
            filtered.append(slot)
    return filtered
def collect_matching_slots(schedule_payload: Dict[str, Any], rules: Optional[List[Dict[str, Any]]]) -> List[Tuple[str, str, str]]:
    """Собирает ВСЕ подходящие (по rules) будущие слоты из payload.

    Возвращает список кортежей (display, start_iso, end_iso) отсортированный по времени.
    display: "YYYY-MM-DD HH:MM".
    Логика фильтрации идентична find_first_matching_slot, чтобы исключить расхождения.
    """
    if not schedule_payload:
        return []

    collected: List[Tuple[datetime, str, str]] = []
    now_utc = datetime.now()

    for day in schedule_payload.get("scheduleOfDay", []):
        for slot_block in day.get("scheduleBySlot", []):
            for slot in slot_block.get("slot", []):
                start_iso = slot.get("startTime")
                if not start_iso:
                    continue
                try:
                    start_dt = datetime.fromisoformat(start_iso)
                except ValueError:
                    continue
                end_iso = slot.get("endTime") or ""

                cmp_dt = start_dt
                # Сравниваем с now в той же таймзоне, если она присутствует
                if start_dt.tzinfo is not None:
                    now_cmp = datetime.now(start_dt.tzinfo)
                else:
                    now_cmp = now_utc

                if cmp_dt <= now_cmp:
                    continue
                if not slot_matches_tracking_rules(cmp_dt, rules):
                    continue

                collected.append((cmp_dt, start_iso, end_iso))

    if not collected:
        return []

    collected.sort(key=lambda x: x[0])
    result: List[Tuple[str, str, str]] = []
    for dt_obj, start_iso, end_iso in collected:
        result.append((dt_obj.strftime("%Y-%m-%d %H:%M"), start_iso, end_iso))
    return result


def find_first_matching_slot(schedule_payload: Dict[str, Any], rules: Optional[List[Dict[str, Any]]]) -> Optional[Tuple[str, str, str]]:
    if not schedule_payload:
        return None

    best: Optional[Tuple[datetime, str, str]] = None
    now_utc = datetime.now()

    for day in schedule_payload.get("scheduleOfDay", []):
        for slot_block in day.get("scheduleBySlot", []):
            for slot in slot_block.get("slot", []):
                start_iso = slot.get("startTime")
                if not start_iso:
                    continue
                try:
                    start_dt = datetime.fromisoformat(start_iso)
                except ValueError:
                    continue

                end_iso = slot.get("endTime")

                cmp_dt = start_dt
                if start_dt.tzinfo is not None:
                    now_cmp = datetime.now(start_dt.tzinfo)
                else:
                    now_cmp = now_utc

                if cmp_dt <= now_cmp:
                    continue
                if not slot_matches_tracking_rules(cmp_dt, rules):
                    continue

                if not best or cmp_dt < best[0]:
                    best = (cmp_dt, start_iso, end_iso or "")

    if not best:
        return None

    display = best[0].strftime("%Y-%m-%d %H:%M")
    return display, best[1], best[2]


def parse_schedule_payload(payload: List[Dict]) -> Set[str]:
    """
    Преобразует payload расписания в множество слотов вида "YYYY-MM-DD HH:MM".
    """
    slots_set = set()
    for day_info in payload:
        schedule_by_slot = day_info.get("scheduleBySlot", [])
        for slot_block in schedule_by_slot:
            for s in slot_block.get("slot", []):
                start_time = s.get("startTime", "");  # пример: "2025-03-24T18:15:00+03:00"
                if len(start_time) >= 16:
                    dt_str = f"{start_time[:10]} {start_time[11:16]}"  # "YYYY-MM-DD HH:MM"
                    slots_set.add(dt_str)
    return slots_set


def group_slots_by_date(slots: Set[str]) -> str:
    """
    Группирует слоты по дате и выводит их в человекочитаемом формате:
    25 марта:
    • 15:48
    • 16:00
    """
    grouped = defaultdict(list)

    for slot in sorted(slots):
        date_part, time_part = slot.split()
        year, month, day = date_part.split("-")
        human_date = f"{int(day)} {MONTHS[month]}"
        grouped[human_date].append(time_part)

    lines = []
    for date, times in grouped.items():
        times.sort()
        lines.append(f"{date}:")
        lines.extend(f"• {time}" for time in times)
    return "\n".join(lines)


def compare_schedules_payloads(
        old_payload: List[Dict],
        new_payload: List[Dict]
) -> Optional[Tuple[Set[str], Set[str], str]]:
    """
    Сравнивает старое и новое расписание и возвращает:
    - добавленные слоты
    - удалённые слоты
    - человекочитаемую строку об изменениях (или None, если изменений нет)
    """
    old_slots = parse_schedule_payload(old_payload) if old_payload else set()
    new_slots = parse_schedule_payload(new_payload) if new_payload else set()

    added_slots = new_slots - old_slots
    removed_slots = old_slots - new_slots

    if not added_slots and not removed_slots:
        return set(), set(), None

    changes = []

    if removed_slots:
        removed_text = group_slots_by_date(removed_slots)
        changes.append(f"❌ <b>Удалены слоты:</b>\n{removed_text}")

    if added_slots:
        added_text = group_slots_by_date(added_slots)
        changes.append(f"📌 <b>Добавлены слоты:</b>\n{added_text}")

    changes_text = "\n\n".join(changes)
    return added_slots, removed_slots, changes_text


async def book_appointment(user_id: int, doctor_api_id: str, slot: str) -> tuple[bool, str | None]:
    """
    Пытается записать пользователя на слот или перенести существующую запись.
    - Ищет врача в БД (`DoctorInfo`) по doctor_api_id.
    - Запрашивает расписание через get_available_resource_schedule_info и находит слот по строке
      формата "YYYY-MM-DD HH:MM".
    - Если у пользователя есть существующая запись (UserDoctorLink для эквивалентных кодов специальности),
      делает shiftAppointment, иначе вызывает createAppointment.
    - Сохраняет/обновляет `UserDoctorLink.appointment_id` при успешной операции.

    Возвращает True при успешной записи/переносе, иначе False.
    """
    # Импортируем здесь, чтобы не поломать порядок импортов в модуле
    from emias_api import get_available_resource_schedule_info, create_appointment, shift_appointment, get_appointment_receptions_by_patient
    from database import get_db_session, DoctorInfo, UserDoctorLink, Specialty, get_equivalent_speciality_codes

    session = get_db_session()
    try:
        doctor = session.query(DoctorInfo).filter_by(doctor_api_id=str(doctor_api_id)).first()
        if not doctor:
            return False, "Врач не найден в базе данных"
        if not doctor.complex_resource_id:
            return False, "Недостаточно данных о враче"

        try:
            available_resource_id = int(doctor.doctor_api_id)
        except Exception:
            # Если doctor_api_id не число, пробуем привести как есть
            try:
                available_resource_id = int(str(doctor.doctor_api_id))
            except Exception:
                return False

        try:
            complex_resource_id = int(doctor.complex_resource_id)
        except Exception:
            complex_resource_id = int(str(doctor.complex_resource_id)) if doctor.complex_resource_id else None

        # Проверяем, есть ли у пользователя запись к специальности врача через API
        logging.info(f"Doctor ar_speciality_id: {doctor.ar_speciality_id}, equivalent codes: {get_equivalent_speciality_codes(doctor.ar_speciality_id)}")
        appointment_id = None
        appointments_data = get_appointment_receptions_by_patient(user_id)
        if appointments_data:
            appointments = appointments_data.get("appointments") or appointments_data.get("appointment") or []
            logging.info(f"User has {len(appointments)} appointments")
            for appt in appointments:
                appt_spec_id = extract_speciality_id_from_appointment(appt)
                appt_id_value = appt.get("appointmentId") or appt.get("id")
                logging.info(f"Appointment spec: {appt_spec_id}, id: {appt_id_value}")
                if appt_spec_id and appt_spec_id in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                    appointment_id = appt_id_value
                    if appointment_id:
                        try:
                            appointment_id = int(appointment_id)
                            break
                        except Exception:
                            appointment_id = appointment_id
                            break

        # Если не нашли через API, проверим в DB
        if not appointment_id:
            for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                if link and link.appointment_id:
                    try:
                        appointment_id = int(link.appointment_id)
                        break
                    except (ValueError, TypeError) as e:
                        logging.error(f"Ошибка конвертации appointment_id из БД {link.appointment_id}: {e}")
                        continue

        if appointment_id:
            schedule_response = get_available_resource_schedule_info(user_id, available_resource_id, complex_resource_id, appointment_id=appointment_id)
        else:
            schedule_response = get_available_resource_schedule_info(user_id, available_resource_id, complex_resource_id)
        if not schedule_response or not schedule_response.get("payload") or not schedule_response.get("payload").get("scheduleOfDay"):
            error_desc = schedule_response.get("Описание") if schedule_response else None
            if not error_desc and schedule_response and schedule_response.get("payload"):
                error_desc = schedule_response.get("payload").get("Описание")
            error_msg = error_desc or "Не удалось получить расписание для врача"
            try:
                log_user_action(session, user_id, 'api_get_schedule_fail', f'Доктор {doctor_api_id}: {error_msg}', source='bot', status='error')
            except Exception:
                pass
            return False, error_msg

        # Формат входного slot: "YYYY-MM-DD HH:MM" -> сравниваем по префиксу ISO "YYYY-MM-DDTHH:MM"
        target_prefix = slot.replace(" ", "T")[:16]
        start_iso = None
        end_iso = None

        for day in schedule_response.get("payload").get("scheduleOfDay", []):
            for slot_block in day.get("scheduleBySlot", []):
                for s in slot_block.get("slot", []):
                    st = s.get("startTime")
                    if not st:
                        continue
                    if st[:16] == target_prefix:
                        start_iso = st
                        end_iso = s.get("endTime")
                        break
                if start_iso:
                    break
            if start_iso:
                break

        if not start_iso or not end_iso:
            try:
                log_user_action(session, user_id, 'api_slot_not_found', f'Доктор {doctor_api_id} слот {slot}', source='bot', status='warning')
            except Exception:
                pass
            return False

        # Определяем reception_type_id (только из Specialty – расписание его не содержит)
        reception_type_id = 0
        try:
            if doctor.ar_speciality_id:
                spec = session.query(Specialty).filter_by(code=doctor.ar_speciality_id).first()
                if not spec:
                    # Автоматически создаём Specialty, если отсутствует (например, новый ldpType)
                    spec = Specialty(code=doctor.ar_speciality_id, name=doctor.ar_speciality_name or doctor.ar_speciality_id)
                    session.add(spec)
                    session.commit()
                if spec and spec.reception_type_id not in (None, ""):
                    try:
                        reception_type_id = int(spec.reception_type_id)
                    except Exception:
                        reception_type_id = 0
                else:
                    try:
                        log_user_action(session, user_id, 'api_reception_type_missing_db', f'Доктор {doctor_api_id} spec {doctor.ar_speciality_id}', source='bot', status='info')
                    except Exception:
                        pass
        except Exception as rt_err:
            try:
                log_user_action(session, user_id, 'api_reception_type_fail', f'Доктор {doctor_api_id} err={rt_err}', source='bot', status='warning')
            except Exception:
                pass

        # Логируем попытку (shift или create)
        try:
            if appointment_id:
                log_user_action(session, user_id, 'api_shift_attempt', f'Доктор {doctor_api_id} слот {slot}', source='bot', status='info')
            else:
                log_user_action(session, user_id, 'api_create_attempt', f'Доктор {doctor_api_id} слот {slot}', source='bot', status='info')
        except Exception:
            pass
        # Если есть существующая запись — пробуем перенести
        if appointment_id:
            resp = shift_appointment(user_id, available_resource_id, complex_resource_id, start_iso, end_iso, appointment_id, reception_type_id)
            if resp and ("payload" in resp or "appointmentId" in resp):
                # Обновляем appointment_id, если новый
                new_id = None
                if isinstance(resp, dict):
                    new_id = resp.get("appointmentId") or (resp.get("payload") and resp.get("payload").get("appointmentId"))
                if new_id:
                    for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                        link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                        if link:
                            link.appointment_id = str(new_id)
                        else:
                            session.add(UserDoctorLink(telegram_user_id=user_id, doctor_speciality=spec_code, appointment_id=str(new_id)))
                    session.commit()
                doc = session.query(DoctorInfo).filter_by(doctor_api_id=str(doctor_api_id)).first()
                title = f"{doc.name} ({doc.ar_speciality_name})" if doc else doctor_api_id
                log_user_action(session, user_id, 'api_shift_appointment', f'Перенос к врачу {title} на {slot}', source='bot', status='success')
                return True, "shift"
            else:
                error_message = resp.get("Описание", "Неизвестная ошибка") if resp else "Нет ответа от сервера"
                try:
                    log_user_action(session, user_id, 'api_shift_appointment_fail', f'Доктор {doctor_api_id} слот {slot} ошибка: {error_message}', source='bot', status='error')
                except Exception:
                    pass
                return False, error_message

        # Иначе — пытаемся создать новую запись
        # Проверяем необходимость направления: если нет существующей записи и нет referral_id в связке
        if not appointment_id:
            try:
                # Политики: 0 strict, 1 fallback, 2 always_allow
                referral_policy = 0
                has_referral = False
                if doctor.ar_speciality_id:
                    spec_row = session.query(Specialty).filter_by(code=doctor.ar_speciality_id).first()
                    if spec_row and hasattr(spec_row, 'referral_policy') and spec_row.referral_policy is not None:
                        try:
                            referral_policy = int(spec_row.referral_policy)
                        except Exception:
                            referral_policy = 0
                # Быстрая проверка whitelist
                if doctor.ar_speciality_id in DISPENSARY_WHITELIST:
                    referral_policy = 2  # treat as always_allow
                # Проверяем наличие referral в связках
                if doctor.ar_speciality_id:
                    for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                        link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                        if link and link.referral_id:
                            has_referral = True
                            break
                # Решение
                if referral_policy == 0:  # strict
                    if not has_referral:
                        log_user_action(session, user_id, 'api_create_referral_required', f'doctor={doctor_api_id} slot={slot}', source='bot', status='error')
                        return False, 'Требуется направление для записи'
                elif referral_policy == 1:  # fallback
                    if not has_referral:
                        # просто логируем инфо, но не блокируем
                        log_user_action(session, user_id, 'api_create_referral_fallback_try', f'doctor={doctor_api_id} slot={slot}', source='bot', status='info')
                else:
                    # always_allow – ничего не делаем
                    pass
            except Exception as _ref_err:
                try:
                    log_user_action(session, user_id, 'api_create_referral_policy_err', f'doc={doctor_api_id} err={_ref_err}', source='bot', status='warning')
                except Exception:
                    pass
        resp = create_appointment(user_id, available_resource_id, complex_resource_id, start_iso, end_iso, reception_type_id)
        if resp and ("payload" in resp or "appointmentId" in resp):
            new_id = None
            if isinstance(resp, dict):
                new_id = resp.get("appointmentId") or (resp.get("payload") and resp.get("payload").get("appointmentId")) or (resp.get("data") and resp.get("data").get("appointmentId"))

            # Сохраняем appointment_id для всех эквивалентных кодов специальности
            if new_id:
                for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                    link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                    if link:
                        link.appointment_id = str(new_id)
                    else:
                        session.add(UserDoctorLink(telegram_user_id=user_id, doctor_speciality=spec_code, appointment_id=str(new_id)))
                session.commit()
            log_user_action(session, user_id, 'api_create_appointment', f'Запись к врачу {doctor_api_id} на {slot}', source='bot', status='success')
            return True, "create"
        else:
            # Попробуем найти appointment_id снова через API
            appointments_data = get_appointment_receptions_by_patient(user_id)
            if appointments_data:
                appointments = appointments_data.get("appointments") or appointments_data.get("appointment") or []
                for appt in appointments:
                    appt_spec_id = str(appt.get("specialityId", ""))
                    if appt_spec_id in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                        appointment_id = appt.get("appointmentId") or appt.get("id")
                        if appointment_id:
                            try:
                                appointment_id = int(appointment_id)
                                break
                            except Exception:
                                appointment_id = appointment_id
            if appointment_id:
                resp2 = shift_appointment(user_id, available_resource_id, complex_resource_id, start_iso, end_iso, appointment_id, reception_type_id)
                if resp2 and ("payload" in resp2 or "appointmentId" in resp2):
                    # Обновляем appointment_id, если новый
                    new_id = None
                    if isinstance(resp2, dict):
                        new_id = resp2.get("appointmentId") or (resp2.get("payload") and resp2.get("payload").get("appointmentId"))
                    if new_id:
                        for spec_code in get_equivalent_speciality_codes(doctor.ar_speciality_id):
                            link = session.query(UserDoctorLink).filter_by(telegram_user_id=user_id, doctor_speciality=spec_code).first()
                            if link:
                                link.appointment_id = str(new_id)
                            else:
                                session.add(UserDoctorLink(telegram_user_id=user_id, doctor_speciality=spec_code, appointment_id=str(new_id)))
                        session.commit()
                    doc = session.query(DoctorInfo).filter_by(doctor_api_id=str(doctor_api_id)).first()
                    title = f"{doc.name} ({doc.ar_speciality_name})" if doc else doctor_api_id
                    log_user_action(session, user_id, 'api_shift_appointment', f'Перенос к врачу {title} на {slot}', source='bot', status='success')
                    return True, "shift"
                else:
                    error_message = resp2.get("Описание", "Неизвестная ошибка") if resp2 else "Нет ответа от сервера"
                    try:
                        log_user_action(session, user_id, 'api_shift_appointment_fail', f'Доктор {doctor_api_id} слот {slot} ошибка: {error_message}', source='bot', status='error')
                    except Exception:
                        pass
                    return False, error_message
            else:
                error_message = resp.get("Описание", "Неизвестная ошибка") if resp else "Нет ответа от сервера"
                try:
                    log_user_action(session, user_id, 'api_create_appointment_fail', f'Доктор {doctor_api_id} слот {slot} ошибка: {error_message}', source='bot', status='error')
                except Exception:
                    pass
                return False, error_message
    finally:
        session.close()


def start_schedule_checker(interval_seconds: int = 60):
    """Запускает планировщик задач, выполняющий check_schedule_updates каждые interval_seconds.
    Предотвращает повторную регистрацию задания, если оно уже добавлено.
    """
    try:
        # Если задание уже добавлено — ничего не делаем
        if scheduler.get_job('schedule_checker'):
            logging.info("Schedule checker already running")
            return
        scheduler.add_job(check_schedule_updates, 'interval', seconds=interval_seconds, id='schedule_checker', max_instances=1)
        scheduler.start()
        logging.info(f"Schedule checker started (interval={interval_seconds}s)")
    except Exception as e:
        logging.error(f"Failed to start schedule checker: {e}")


async def main():
    """Запускает бота и планировщик"""
    # Настройка логирования
    logging.basicConfig(
        filename='bot.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )
    print("Запускаем бота... ✅")
    register_handlers(dp)

    # Устанавливаем команды для меню
    commands = [
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="auth", description="Авторизация через токены"),
        BotCommand(command="help", description="Помощь"),
        BotCommand(command="whoami", description="Информация о пользователе"),
        BotCommand(command="get_profile_info", description="Получить информацию о профиле"),
        BotCommand(command="register_profile", description="Регистрация профиля (ОМС, дата рождения)"),
        BotCommand(command="get_receptions", description="Получить приёмы"),
        BotCommand(command="get_referrals", description="Получить направления"),
        BotCommand(command="get_specialities", description="Получить специальности"),
        BotCommand(command="get_doctors_info", description="Информация о врачах"),
        BotCommand(command="get_clinics", description="Получить клиники"),
        BotCommand(command="favourites", description="Избранное"),
        BotCommand(command="tracked", description="Отслеживаемые врачи"),
    ]
    await bot.set_my_commands(commands)

    # Выполняем первую проверку расписания сразу при запуске
    await check_schedule_updates()

    # Запуск фонового планировщика
    start_schedule_checker()

    # Стартуем бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
