from flask import Flask, render_template, request, redirect, url_for, session, flash
from database import (
    get_db_session,
    UserProfile,
    UserToken,
    UserTrackedDoctor,
    DoctorInfo,
    UserFavoriteDoctor,
    UserLog,
    log_user_action,
    engine,
    Base,
    DoctorSchedule,
    save_tokens,
    Specialty,
    UserDoctorLink
)
from database import LPUAddress
from emias_api import (
    get_whoami,
    get_assignments_referrals_info,
    get_available_resource_schedule_info,
    get_appointment_receptions_by_patient,
    refresh_emias_token,
)
import json, datetime as dt
from datetime import timezone, timedelta
import os
from sqlalchemy import text, or_, func

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'default_secret_key')

# ---- Вспомогательные функции (восстановленные) ----
def format_tracking_rules(rules):
    if not rules:
        return 'Нет'
    formatted = []
    for rule in rules:
        if isinstance(rule, dict):
            value = rule.get('value', '')
            time_ranges = rule.get('timeRanges', [])
            if time_ranges:
                for tr in time_ranges:
                    formatted.append(f"{value}: {tr}")
            else:
                formatted.append(value)
        else:
            formatted.append(str(rule))
    return ', '.join(formatted)

@app.template_filter('format_rules')
def format_rules_filter(rules):
    return format_tracking_rules(rules)

def get_user_profile(user_id):
    session_db = get_db_session()
    profile = session_db.query(UserProfile).filter_by(telegram_user_id=user_id).first()
    session_db.close()
    return profile

def is_admin(user_id):
    profile = get_user_profile(user_id)
    return profile and profile.is_admin

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('user_dashboard'))
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        try:
            user_id = int(username)
        except ValueError:
            flash('Неверный username')
            return render_template('login.html')
        profile = get_user_profile(user_id)
        if profile and profile.password == password:
            session['user_id'] = user_id
            return redirect(url_for('user_dashboard'))
        else:
            flash('Неверный username или пароль')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect(url_for('index'))

@app.route('/user')
def user_dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    profile = get_user_profile(user_id)
    session_db = get_db_session()
    tokens = session_db.query(UserToken).filter_by(telegram_user_id=user_id).first()
    tracked = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id).all()
    favorites = session_db.query(UserFavoriteDoctor).filter_by(telegram_user_id=user_id).all()
    
    # Получить информацию о врачах
    tracked_ids = [t.doctor_api_id for t in tracked]
    favorite_ids = [f.doctor_api_id for f in favorites]
    all_ids = set(tracked_ids + favorite_ids)
    doctors = session_db.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.in_(all_ids)).all()
    doctor_dict = {d.doctor_api_id: d for d in doctors}
    # Prefetch LPU short names for these doctors (address_point_id -> short_name)
    from database import LPUAddress
    ap_ids = {d.address_point_id for d in doctors if d.address_point_id}
    lpu_map = {}
    if ap_ids:
        addr_rows = session_db.query(LPUAddress).filter(LPUAddress.address_point_id.in_(ap_ids)).all()
        lpu_map = {a.address_point_id: a.short_name or a.address for a in addr_rows}
    
    # Логи токенов: ищем последний success и последний error для api_refresh_token
    last_refresh_log = session_db.query(UserLog).filter_by(telegram_user_id=user_id, action='api_refresh_token').order_by(UserLog.timestamp.desc()).first()
    last_success_refresh = session_db.query(UserLog).filter_by(telegram_user_id=user_id, action='api_refresh_token', status='success').order_by(UserLog.timestamp.desc()).first()
    last_error_refresh = session_db.query(UserLog).filter_by(telegram_user_id=user_id, action='api_refresh_token', status='error').order_by(UserLog.timestamp.desc()).first()
    session_db.close()
    token_status = None
    remaining_seconds = None
    last_token_update = None
    token_error = None
    token_error_details = None
    last_refresh_attempt = None
    show_refresh_attempt = False
    issued_at_raw = getattr(tokens, 'issued_at', None) if tokens else None
    if tokens and getattr(tokens, 'expires_at', None):
        MSK = timezone(timedelta(hours=3))
        expires_at_raw = tokens.expires_at
        # Трактуем naive как UTC (старое поведение сохранения) и конвертируем в МСК для отображения
        if expires_at_raw.tzinfo is None:
            expires_at_utc = expires_at_raw.replace(tzinfo=timezone.utc)
        else:
            expires_at_utc = expires_at_raw.astimezone(timezone.utc)
        now_utc = dt.datetime.now(timezone.utc)
        delta = (expires_at_utc - now_utc).total_seconds()
        remaining_seconds = int(delta)
        if delta <= 0:
            token_status = 'expired'
        elif delta < 900:
            token_status = 'soon'
        else:
            token_status = 'ok'
        # Используем issued_at напрямую (теперь колонка обязательна)
        try:
            if issued_at_raw:
                if issued_at_raw.tzinfo is None:
                    issued_utc = issued_at_raw.replace(tzinfo=timezone.utc)
                else:
                    issued_utc = issued_at_raw.astimezone(timezone.utc)
                last_token_update = issued_utc.astimezone(MSK)
        except Exception:
            last_token_update = None
    # Анализ последней попытки refresh: если последний лог об ошибке – показываем пользователю
    # Определяем какая попытка (берём последнюю по времени вообще)
    if last_refresh_log:
        ts = last_refresh_log.timestamp
        if ts:
            if ts.tzinfo is None:
                ts_utc = ts.replace(tzinfo=timezone.utc)
            else:
                ts_utc = ts.astimezone(timezone.utc)
            last_refresh_attempt = ts_utc.astimezone(MSK)

    # Определяем, показывать ли ошибку: только если после неё НЕ было success.
    if last_error_refresh:
        show_error = True
        if last_success_refresh and last_success_refresh.timestamp and last_error_refresh.timestamp:
            if last_success_refresh.timestamp > last_error_refresh.timestamp:
                show_error = False  # ошибка устарела, есть более поздний успех
        if show_error:
            token_error = True
            token_error_details = last_error_refresh.details
            if token_status not in ['expired']:
                token_status = 'error'
    # Решение дублирования: показываем строку "Последняя попытка refresh" только если:
    #  - есть ошибка (token_error)
    #  - или нет issued_at (last_token_update is None)
    #  - или время попытки существенно отличается (>2 сек) от issued_at
    try:
        if last_refresh_attempt:
            if token_error or not last_token_update:
                show_refresh_attempt = True
            else:
                diff = abs(int(last_refresh_attempt.timestamp()) - int(last_token_update.timestamp()))
                if diff > 2:
                    show_refresh_attempt = True
    except Exception:
        pass
    return render_template('user_dashboard.html', profile=profile, tokens=tokens, tracked=tracked, favorites=favorites, doctor_dict=doctor_dict, lpu_map=lpu_map, token_status=token_status, remaining_seconds=remaining_seconds, last_token_update=last_token_update, token_error=token_error, token_error_details=token_error_details, last_refresh_attempt=last_refresh_attempt, show_refresh_attempt=show_refresh_attempt)

@app.route('/user/refresh_token', methods=['POST'])
def manual_refresh_token():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    # Принудительная попытка обновить токен даже если он ещё не истёк (force=True)
    sess = get_db_session()
    current = sess.query(UserToken).filter_by(telegram_user_id=user_id).first()
    old_access = current.access_token if current else None
    sess.close()
    new_access = refresh_emias_token(user_id, source='web', force=True)
    if new_access is None:
        flash('Не удалось обновить токен (см. лог).', 'danger')
    else:
        if old_access and new_access == old_access:
            flash('Принудительный запрос выполнен: токен уже был актуален (сервер вернул тот же).', 'info')
        else:
            flash('Токен успешно обновлён (force).', 'success')
    return redirect(url_for('user_dashboard'))
    return render_template('user_dashboard.html', profile=profile, tokens=tokens, tracked=tracked, favorites=favorites, doctor_dict=doctor_dict, token_status=token_status, remaining_seconds=remaining_seconds, last_token_update=last_token_update)

## Удалены все маршруты /lpu* (по запросу)

@app.route('/diagnostics')
def diagnostics_index():
    """Список диагностических кодов (УЗИ, ЭКГ и т.п.) найденных в базе.
    Формируется автоматически по названиям Specialty / DoctorInfo.ar_speciality_name.
    """
    if 'user_id' not in session:
        return redirect(url_for('login'))
    keywords = [
        'узи', 'ультраз', 'эхо', 'эхокар', 'экг', 'cут', 'холтер', 'дуплекс', 'допплер'
    ]
    sess = get_db_session()
    try:
        # Собираем все специальности и врачей
        specs = sess.query(Specialty).all()
        # map code -> name
        diag_specs = {}
        for sp in specs:
            nm = (sp.name or '').lower()
            if any(k in nm for k in keywords):
                diag_specs[sp.code] = sp.name
        # Добавим из DoctorInfo (на случай если спец ещё не в Specialty)
        docs = sess.query(DoctorInfo).all()
        for d in docs:
            nm = (d.ar_speciality_name or '').lower()
            if nm and any(k in nm for k in keywords):
                diag_specs.setdefault(d.ar_speciality_id, d.ar_speciality_name)
        # Подсчёт сколько ресурсов по коду
        counts = {c: 0 for c in diag_specs}
        for d in docs:
            if d.ar_speciality_id in counts:
                counts[d.ar_speciality_id] += 1
        result = [
            {'code': code, 'name': diag_specs[code], 'count': counts.get(code, 0)}
            for code in sorted(diag_specs, key=lambda x: (diag_specs[x] or '', x)) if code
        ]
    finally:
        sess.close()
    return render_template('diagnostics.html', diagnostics=result)

@app.route('/user/update_tokens', methods=['POST'])
def user_update_tokens():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    raw = request.form.get('tokens_json', '').strip()
    if not raw:
        flash('Пустой ввод', 'warning')
        return redirect(url_for('user_dashboard'))
    try:
        data = json.loads(raw)
        access = data.get('access_token') or data.get('accessToken')
        refresh = data.get('refresh_token') or data.get('refreshToken')
        expires_in = int(data.get('expires_in') or data.get('expiresIn') or 3600)
        if not access or not refresh:
            raise ValueError('Нужны access_token и refresh_token')
        sess = get_db_session()
        save_tokens(sess, user_id, access, refresh, expires_in)
        log_user_action(sess, user_id, 'manual_token_update', f'Обновлены токены вручную expires_in={expires_in}', source='web', status='success')
        sess.close()
        flash('Токены обновлены', 'success')
    except Exception as e:
        flash(f'Ошибка: {e}', 'danger')
        try:
            sess = get_db_session()
            log_user_action(sess, user_id, 'manual_token_update', f'Ошибка: {e}', source='web', status='error')
            sess.close()
        except Exception:
            pass
    return redirect(url_for('user_dashboard'))

@app.route('/user/toggle_auto/<doctor_id>', methods=['POST'])
def toggle_auto(doctor_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    try:
        track = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
        if track:
            track.auto_booking = not track.auto_booking
            doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
            doctor_name = doctor.name if doctor else doctor_id
            status = 'включена' if track.auto_booking else 'выключена'
            log_user_action(session_db, user_id, 'Переключение автозаписи', f'Врач: {doctor_name}, теперь: {status}', source='web', status='info')
            session_db.commit()
    finally:
        session_db.close()
    return redirect(url_for('user_dashboard'))

@app.route('/admin/')
def admin_dashboard():
    if 'user_id' not in session or not is_admin(session['user_id']):
        return redirect(url_for('login'))
    session_db = get_db_session()
    users = session_db.query(UserProfile).all()
    doctors = session_db.query(DoctorInfo).all()
    session_db.close()
    return render_template('admin_dashboard.html', users=users, doctors=doctors, models=ADMIN_MODELS)


def _get_ldp_specialty_codes(sess):
    """Возвращает множество кодов специальностей для LDP (по врачам с id начинающимся на ldp:)."""
    try:
        q = sess.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.like('ldp:%'))
        codes = set()
        for d in q.all():
            if d.ar_speciality_id:
                codes.add(str(d.ar_speciality_id))
        return codes
    except Exception:
        return set()

@app.route('/admin/bulk', methods=['POST'])
def admin_bulk():
    if not _admin_required():
        return redirect(url_for('login'))
    action = request.form.get('action')
    sess = get_db_session()
    try:
        if action == 'set_ldp_policy':
            try:
                policy = int(request.form.get('policy', 1))
            except Exception:
                policy = 1
            codes = _get_ldp_specialty_codes(sess)
            if not codes:
                flash('LDP специальности не найдены', 'warning')
            else:
                updated = sess.query(Specialty).filter(Specialty.code.in_(list(codes))).update({'referral_policy': policy}, synchronize_session=False)
                sess.commit()
                flash(f'Обновлено LDP специальностей: {updated} (policy={policy})', 'success')
                try:
                    log_user_action(sess, session.get('user_id'), 'admin_bulk', f'set_ldp_policy policy={policy} updated={updated}', source='web', status='info')
                except Exception:
                    pass
        elif action == 'trim_logs':
            try:
                keep = int(request.form.get('keep', 500))
            except Exception:
                keep = 500
            # Оставляем последние keep логов глобально
            ids_keep = [row[0] for row in sess.query(UserLog.id).order_by(UserLog.timestamp.desc()).limit(keep).all()]
            if ids_keep:
                deleted = sess.query(UserLog).filter(~UserLog.id.in_(ids_keep)).delete(synchronize_session=False)
            else:
                deleted = 0
            sess.commit()
            flash(f'Оставлено {keep}, удалено {deleted} логов', 'warning')
            try:
                log_user_action(sess, session.get('user_id'), 'admin_bulk', f'trim_logs keep={keep} deleted={deleted}', source='web', status='warning')
            except Exception:
                pass
        elif action == 'delete_logs_older':
            try:
                days = int(request.form.get('days', 30))
            except Exception:
                days = 30
            cutoff = dt.datetime.utcnow() - dt.timedelta(days=days)
            deleted = sess.query(UserLog).filter(UserLog.timestamp < cutoff).delete(synchronize_session=False)
            sess.commit()
            flash(f'Удалено старых логов: {deleted} (старше {days} дн.)', 'warning')
            try:
                log_user_action(sess, session.get('user_id'), 'admin_bulk', f'delete_logs_older days={days} deleted={deleted}', source='web', status='warning')
            except Exception:
                pass
        elif action == 'delete_user_logs':
            try:
                target_user_id = int(request.form.get('target_user_id'))
            except Exception:
                target_user_id = None
            if not target_user_id:
                flash('Не указан user id', 'danger')
            else:
                deleted = sess.query(UserLog).filter(UserLog.telegram_user_id == target_user_id).delete(synchronize_session=False)
                sess.commit()
                flash(f'Удалено логов пользователя {target_user_id}: {deleted}', 'warning')
                try:
                    log_user_action(sess, session.get('user_id'), 'admin_bulk', f'delete_user_logs user={target_user_id} deleted={deleted}', source='web', status='warning')
                except Exception:
                    pass
        else:
            flash('Неизвестное действие', 'danger')
    finally:
        sess.close()
    return redirect(url_for('admin_dashboard'))


# --- Generic Admin CRUD ---

def _admin_required():
    if 'user_id' not in session or not is_admin(session['user_id']):
        return False
    return True

ADMIN_MODELS = {
    'specialty': {
        'model': Specialty,
        'title': 'Специальности / LDP типы',
        'editable': ['code','name','referral_policy','reception_type_id','ar_inquiry_purpose_code','ar_inquiry_purpose_id','appointment_duration'],
        'create': True,
        'delete': True,
        'order_by': 'code'
    },
    'user': {
        'model': UserProfile,
        'title': 'Пользователи',
        # Делаем только просмотр – не редактируем из generic CRUD
        'editable': [],
        'create': False,
        'delete': False,
        'order_by': 'telegram_user_id'
    },
    'doctor': {
        'model': DoctorInfo,
        'title': 'Врачи/Ресурсы',
        # Добавили address_point_id для редактирования места приёма
        'editable': ['name','ar_speciality_id','ar_speciality_name','complex_resource_id','address_point_id'],
        'create': False,
        'delete': False,
        'order_by': 'doctor_api_id'
    },
    'address': {
        'model': LPUAddress,
        'title': 'Адреса (LPUAddress)',
        # Разрешим редактировать short_name и lpu_id, полный address пока только для просмотра (часто из API)
        'editable': ['short_name','lpu_id'],
        'create': False,  # Создание вручную отключено чтобы не плодить «висячие» записи
        'delete': False,  # Удаление отключено: может сломать привязки doctor.address_point_id
        'order_by': 'address_point_id'
    },
    'tracked': {
        'model': UserTrackedDoctor,
        'title': 'Отслеживаемые врачи',
        'editable': ['auto_booking','active','tracking_rules'],
        'create': False,
        'delete': True,
        'order_by': 'telegram_user_id'
    },
    'favorite': {
        'model': UserFavoriteDoctor,
        'title': 'Избранные врачи',
        'editable': [],
        'create': False,
        'delete': True,
        'order_by': 'telegram_user_id'
    },
    'link': {
        'model': UserDoctorLink,
        'title': 'Связки пользователь-специальность',
        'editable': ['appointment_id','referral_id'],
        'create': False,
        'delete': True,
        'order_by': 'telegram_user_id'
    },
    'log': {
        'model': UserLog,
        'title': 'Логи',
        'editable': [],
        'create': False,
        'delete': True,
        'order_by': 'timestamp'
    },
    'schedule': {
        'model': DoctorSchedule,
        'title': 'Врачи расписания',
        'editable': [],  # только просмотр
        'create': False,
        'delete': True,   # позволим чистить кеш расписаний вручную
        'order_by': 'updated_at'
    }
}

def _instance_label(model_key, obj):
    """Упрощённая подпись экземпляра: во всех случаях, если есть поле name — берём его.
    Иначе используем fallback по ключевым идентификаторам.
    Пользователь уточнил: ФИО это name и почти везде поле называется name, поэтому без лишних конструкций.
    """
    if not obj:
        return ''
    try:
        name_val = getattr(obj, 'name', None)
        if name_val:
            return str(name_val)
        # fallback generic по важным атрибутам
        for attr in ('code','doctor_api_id','telegram_user_id','ar_speciality_name','action'):
            v = getattr(obj, attr, None)
            if v:
                return str(v)
    except Exception:
        return ''
    return ''

def _coerce_value(current, value: str):
    if value == '':
        return None
    if isinstance(current, bool):
        return value.lower() in ('1','true','yes','on')
    if isinstance(current, int):
        try:
            return int(value)
        except Exception:
            return current
    # JSON field detection
    import json as _json
    if isinstance(current, list) or (value.strip().startswith('[') and value.strip().endswith(']')):
        try:
            return _json.loads(value)
        except Exception:
            return current
    return value


@app.route('/admin/model/<model_key>')
def admin_model_list(model_key):
    if not _admin_required():
        return redirect(url_for('login'))
    cfg = ADMIN_MODELS.get(model_key)
    if not cfg:
        return redirect(url_for('admin_dashboard'))
    session_db = get_db_session()
    Model = cfg['model']
    query = session_db.query(Model)

    # Поиск (до 500 записей) — параметр q
    q = (request.args.get('q') or '').strip()
    if q:
        like = f"%{q}%"
        # Lowercase literal once in Python to avoid relying on SQL lower() behavior for Cyrillic
        like_lower = like.lower()
        # Специальная логика для расписаний врачей (schedule): хотим искать по имени врача, специальности и doctor_api_id
        if model_key == 'schedule':
            try:
                query = query.join(DoctorInfo, DoctorSchedule.doctor_api_id == DoctorInfo.doctor_api_id)
                query = query.filter(
                    or_(
                        func.lower(DoctorInfo.name).like(like_lower),
                        func.lower(DoctorInfo.ar_speciality_name).like(like_lower),
                        func.lower(DoctorSchedule.doctor_api_id).like(like_lower)
                    )
                )
            except Exception:
                # Fallback: простейший фильтр по doctor_api_id
                try:
                    query = query.filter(func.lower(DoctorSchedule.doctor_api_id).like(like_lower))
                except Exception:
                    pass
        elif model_key == 'doctor':
            # Для поиска по врачам: SQLite lower()/COLLATE может некорректно обрабатывать кириллицу.
            # Поэтому не фильтруем по `name`/`ar_speciality_name` на SQL-уровне — это сделает
            # Python-side casefold фильтр ниже. Единственный SQL-фильтр, который надёжно
            # работает, это по doctor_api_id (цифровой идентификатор).
            try:
                # Если пользователь ввёл только цифры (или строку содержащую цифры),
                # ограничим выборку по doctor_api_id для эффективности.
                if q.isdigit():
                    query = query.filter(DoctorInfo.doctor_api_id.like(like))
                # иначе оставляем фильтрацию по текстовым полям на Python-стороне
            except Exception:
                pass
        else:
            # Универсальный упрощённый поиск: пытаемся угадать популярные поля
            filters = []
            for attr_name in ('name','doctor_api_id','code','ar_speciality_name','action','details'):
                if hasattr(Model, attr_name):
                    try:
                        filters.append(func.lower(getattr(Model, attr_name)).like(like_lower))
                    except Exception:
                        pass
            if filters:
                try:
                    query = query.filter(or_(*filters))
                except Exception:
                    pass
            else:
                # Если вообще ничего не нашли — игнорируем q
                pass

    # Сортировка если указана
    if cfg.get('order_by'):
        try:
            query = query.order_by(getattr(Model, cfg['order_by']))
        except Exception:
            pass

    # Special-case: SQLite's lower()/COLLATE NOCASE may not handle Unicode (Cyrillic) properly.
    # For the `doctor` model, if a free-text q is provided, perform a Python-side
    # casefold() filter to guarantee Unicode-aware case-insensitive matching.
    if model_key == 'doctor' and q:
        try:
            all_items = query.all()
            q_fold = q.casefold()
            filtered = []
            # Prefetch address map for doctors to allow searching by LPUAddress.short_name & full address
            try:
                from database import LPUAddress
                ap_ids_prefetch = {d.address_point_id for d in all_items if getattr(d, 'address_point_id', None)}
                addr_map = {}
                if ap_ids_prefetch:
                    session_db_pref = get_db_session()
                    try:
                        addr_rows_pref = session_db_pref.query(LPUAddress).filter(LPUAddress.address_point_id.in_(ap_ids_prefetch)).all()
                        addr_map = {a.address_point_id: a for a in addr_rows_pref}
                    finally:
                        session_db_pref.close()
            except Exception:
                addr_map = {}
            for it in all_items:
                hay = []
                if getattr(it, 'name', None):
                    hay.append(it.name.casefold())
                if getattr(it, 'ar_speciality_name', None):
                    hay.append(it.ar_speciality_name.casefold())
                if getattr(it, 'doctor_api_id', None):
                    hay.append(str(it.doctor_api_id).casefold())
                # Address fields
                apx = getattr(it, 'address_point_id', None)
                if apx and apx in addr_map:
                    aobj = addr_map.get(apx)
                    if aobj and getattr(aobj, 'short_name', None):
                        try: hay.append(aobj.short_name.casefold())
                        except Exception: pass
                    if aobj and getattr(aobj, 'address', None):
                        try: hay.append(aobj.address.casefold())
                        except Exception: pass
                if any(q_fold in v for v in hay):
                    filtered.append(it)
            total_found = len(filtered)
            items = filtered[:500]
            # Prefetch адресов для результата поиска
            try:
                from database import LPUAddress
                ap_ids = {d.address_point_id for d in items if getattr(d, 'address_point_id', None)}
                if ap_ids:
                    session_db2 = get_db_session()
                    try:
                        addr_rows = session_db2.query(LPUAddress).filter(LPUAddress.address_point_id.in_(ap_ids)).all()
                        amap = {a.address_point_id: a for a in addr_rows}
                        for d in items:
                            ap = getattr(d, 'address_point_id', None)
                            short_addr = ''
                            if ap and ap in amap:
                                # Используем ТОЛЬКО short_name из LPUAddress без попытки сокращать полный адрес
                                short_addr = amap[ap].short_name or ''
                            setattr(d, 'resolved_short_address', short_addr)
                    finally:
                        session_db2.close()
            except Exception:
                pass
            session_db.close()
            return render_template('admin_model_list.html', cfg=cfg, model_key=model_key, items=items, q=q, total_found=total_found)
        except Exception:
            # fallback to SQL-based results if something goes wrong
            pass

    # Считаем количество найденных (до лимита) — отдельный запрос
    try:
        total_found = query.count()
    except Exception:
        total_found = None

    items = query.limit(500).all()
    # Prefetch короткий адрес для doctor: подтягиваем LPUAddress по address_point_id
    if model_key == 'doctor':
        try:
            from database import LPUAddress
            # Соберём уникальные address_point_id
            ap_ids = {d.address_point_id for d in items if getattr(d, 'address_point_id', None)}
            if ap_ids:
                session_db2 = get_db_session()
                try:
                    addr_rows = session_db2.query(LPUAddress).filter(LPUAddress.address_point_id.in_(ap_ids)).all()
                    amap = {a.address_point_id: a for a in addr_rows}
                    for d in items:
                        ap = getattr(d, 'address_point_id', None)
                        short_addr = ''
                        if ap and ap in amap:
                            # Только short_name; не обрезаем полный адрес до города.
                            short_addr = amap[ap].short_name or ''
                        setattr(d, 'resolved_short_address', short_addr)
                finally:
                    session_db2.close()
        except Exception:
            pass
    session_db.close()
    return render_template('admin_model_list.html', cfg=cfg, model_key=model_key, items=items, q=q, total_found=total_found)

@app.route('/admin/model/<model_key>/create', methods=['GET','POST'])
def admin_model_create(model_key):
    if not _admin_required():
        return redirect(url_for('login'))
    cfg = ADMIN_MODELS.get(model_key)
    if not cfg or not cfg.get('create'):
        return redirect(url_for('admin_dashboard'))
    Model = cfg['model']
    if request.method == 'POST':
        session_db = get_db_session()
        obj = Model()
        for field in cfg['editable']:
            if field in request.form:
                raw = request.form.get(field, '')
                current = getattr(obj, field, None)
                setattr(obj, field, _coerce_value(current, raw))
        session_db.add(obj)
        session_db.commit()
        session_db.close()
        return redirect(url_for('admin_model_list', model_key=model_key))
    return render_template('admin_model_form.html', cfg=cfg, model_key=model_key, obj=None)

@app.route('/admin/model/<model_key>/<int:obj_id>/edit', methods=['GET','POST'])
def admin_model_edit(model_key, obj_id):
    if not _admin_required():
        return redirect(url_for('login'))
    cfg = ADMIN_MODELS.get(model_key)
    if not cfg:
        return redirect(url_for('admin_dashboard'))
    Model = cfg['model']
    session_db = get_db_session()
    obj = session_db.query(Model).filter_by(id=obj_id).first()
    if not obj:
        session_db.close()
        return redirect(url_for('admin_model_list', model_key=model_key))
    if request.method == 'POST':
        changed_fields = []
        old_values = {}
        for field in cfg['editable']:
            if field in request.form:
                raw = request.form.get(field, '')
                current = getattr(obj, field, None)
                old_values[field] = current
                new_val = _coerce_value(current, raw)
                if new_val != current:
                    setattr(obj, field, new_val)
                    changed_fields.append(field)
        session_db.commit()
        # Дополнительный лог по смене address_point_id с указанием адресов
        if 'address_point_id' in changed_fields:
            try:
                from database import LPUAddress
                old_id = old_values.get('address_point_id')
                new_id = getattr(obj, 'address_point_id', None)
                addr_old = None
                addr_new = None
                if old_id:
                    addr_old = session_db.query(LPUAddress).filter_by(address_point_id=old_id).first()
                if new_id:
                    addr_new = session_db.query(LPUAddress).filter_by(address_point_id=new_id).first()
                detail = f"address_point_id {old_id} -> {new_id} | old_short={(addr_old.short_name if addr_old else '')} | new_short={(addr_new.short_name if addr_new else '')}"
                try:
                    log_user_action(session_db, session.get('user_id'), 'admin_edit_doctor_address', detail, source='web', status='info')
                except Exception:
                    pass
            except Exception:
                pass
        # Сформировать ярлык до закрытия сессии, чтобы избежать DetachedInstanceError
        try:
            label_for_flash = _instance_label(model_key, obj)
        except Exception:
            label_for_flash = None
        # Логируем до закрытия сессии
        if changed_fields:
            try:
                ident = label_for_flash or f'{model_key}#{obj_id}'
                log_user_action(session_db, session.get('user_id'), 'admin_edit', f'{ident} поля: {", ".join(changed_fields)}', source='web', status='info')
            except Exception:
                pass
        session_db.close()
        # Сообщение и возврат к списку модели
        if changed_fields:
            lbl = label_for_flash
            if model_key == 'doctor':
                if isinstance(lbl, str) and lbl.strip():
                    fio_lower = ' '.join(part for part in lbl.strip().split())  # normalize spaces
                    fio_lower = fio_lower.lower()
                    prefix = f'доктор {fio_lower}'
                else:
                    prefix = 'доктор'
            else:
                prefix = lbl or f'{model_key}#{obj_id}'
            if len(changed_fields) == 1:
                flash(f'{prefix}: изменено поле: {changed_fields[0]}', 'success')
            else:
                flash(f'{prefix}: изменены поля: {", ".join(changed_fields)}', 'success')
        else:
            lbl = label_for_flash
            if model_key == 'doctor':
                if isinstance(lbl, str) and lbl.strip():
                    fio_lower = ' '.join(part for part in lbl.strip().split())
                    fio_lower = fio_lower.lower()
                    prefix = f'доктор {fio_lower}'
                else:
                    prefix = 'доктор'
            else:
                prefix = lbl or f'{model_key}#{obj_id}'
            flash(f'{prefix}: нет изменений', 'info')
        return redirect(url_for('admin_model_list', model_key=model_key))
    # For display we just pass object
    # Подготовим список адресов для выбора если редактируем врача
    addresses = []
    addresses_payload = []
    if model_key == 'doctor':
        try:
            from database import LPUAddress
            # Берём все адреса (при большом количестве можно добавить пагинацию/фильтр)
            # Убрали .nullsLast() — в SQLite/SQLAlchemy может падать; сортируем простым order_by(short_name, address_point_id)
            try:
                addresses = session_db.query(LPUAddress).order_by(LPUAddress.short_name.asc(), LPUAddress.address_point_id.asc()).all()
            except Exception:
                addresses = session_db.query(LPUAddress).all()
            # Лёгкое диагностическое сообщение в stdout (fallback логирование количества адресов)
            try:
                print(f"[admin_model_edit] loaded addresses count={len(addresses)}")
            except Exception:
                pass
            for a in addresses:
                try:
                    addresses_payload.append({
                        'id': a.id,
                        'ap': a.address_point_id or '',
                        'short': a.short_name or '',
                        'full': a.address or '',
                        'lpu': a.lpu_id or ''
                    })
                except Exception:
                    pass
        except Exception:
            try:
                addresses = []
                addresses_payload = []
            except Exception:
                pass
    session_db.close()
    return render_template('admin_model_form.html', cfg=cfg, model_key=model_key, obj=obj, addresses=addresses, addresses_payload=addresses_payload)

@app.route('/admin/tools/backfill_lpu_short_names', methods=['POST','GET'])
def admin_backfill_lpu_short_names():
    """Заполняет LPUAddress.short_name где он пуст, используя /getDoctorsInfo.

    Алгоритм:
      1. Находим LPUAddress без short_name.
      2. Для связанных DoctorInfo собираем пары (speciality_id, lpu_id).
      3. Для каждой пары вызываем getDoctorsInfo (c lpuId если есть).
      4. Прогоняем все availableResources через save_or_update_doctor -> обновляется short_name.
    """
    if not _admin_required():
        return redirect(url_for('login'))
    # Пользуемся текущим админским telegram_user_id для API токенов
    # Веб-сессия хранит 'user_id' (telegram_user_id), используем его
    admin_uid = session.get('user_id')
    if not admin_uid:
        flash('Нет telegram_user_id в сессии — авторизуйтесь как админ.', 'danger')
        return redirect(url_for('admin_dashboard'))
    # Проверим наличие токенов/профиля
    s_chk = get_db_session()
    try:
        from database import get_tokens, get_profile
        if not get_tokens(s_chk, admin_uid) or not get_profile(s_chk, admin_uid):
            flash('У админ-пользователя отсутствуют токены или профиль. Выполните /auth в боте.', 'danger')
            return redirect(url_for('admin_dashboard'))
    finally:
        s_chk.close()

    session_db = get_db_session()
    from database import LPUAddress, save_or_update_doctor
    try:
        # Собираем адреса без short_name
        missing = session_db.query(LPUAddress).filter(or_(LPUAddress.short_name == None, LPUAddress.short_name == '')).all()
        if not missing:
            flash('Нет адресов без short_name — ничего делать не нужно.', 'success')
            return redirect(url_for('admin_model_list', model_key='doctor'))
        # Мапа address_point_id -> (specialities, lpu_id)
        addr_to_specs = {}
        for doc in session_db.query(DoctorInfo).filter(DoctorInfo.address_point_id != None).all():
            ap = doc.address_point_id
            if not ap:
                continue
            addr_to_specs.setdefault(ap, {'specs': set(), 'lpu': None})
            if doc.ar_speciality_id:
                addr_to_specs[ap]['specs'].add(doc.ar_speciality_id)
        # Собираем пары (spec, lpu_id) только для тех address_point_id где short_name пуст
        pairs = set()
        for addr in missing:
            meta = addr_to_specs.get(addr.address_point_id)
            if not meta:
                continue
            lpu_id_val = addr.lpu_id
            for spec in meta['specs']:
                pairs.add((spec, lpu_id_val))
        if not pairs:
            flash('Не удалось собрать пары speciality/lpu для backfill.', 'warning')
            return redirect(url_for('admin_model_list', model_key='doctor'))

        from emias_api import get_doctors_info
        updated_addresses_before = {a.address_point_id: (a.short_name or '') for a in missing}
        api_calls = 0
        updated_after = set()
        # Вспомогательная сессия для сохранения
        for spec, lpu in pairs:
            try:
                resp = get_doctors_info(admin_uid, speciality_id=[str(spec)], lpu_id=str(lpu) if lpu else None)
                api_calls += 1
            except Exception as e:
                flash(f'Ошибка API getDoctorsInfo spec={spec} lpu={lpu}: {e}', 'danger')
                continue
            if not resp or not resp.get('payload'):
                continue
            payload = resp['payload']
            blocks = payload.get('doctorsInfo', [])
            not_av = payload.get('notAvailableDoctors', [])
            for block in blocks:
                block_lpu_id = block.get('lpuId') or block.get('lpuID')
                block_addr = block.get('defaultAddress') or block.get('lpuAddress')
                block_lpu_short = block.get('lpuShortName') or block.get('lpu_short_name')
                for resource in block.get('availableResources', []):
                    if block_lpu_id and not resource.get('lpuId'):
                        resource['lpuId'] = block_lpu_id
                    if block_addr and not (resource.get('lpuAddress') or resource.get('defaultAddress')):
                        resource['lpuAddress'] = block_addr
                    if block_lpu_short and not resource.get('lpuShortName'):
                        resource['lpuShortName'] = block_lpu_short
                    save_or_update_doctor(session_db, admin_uid, resource)
            for doc in not_av:
                save_or_update_doctor(session_db, admin_uid, doc)
            session_db.commit()
        # Повторно загрузим затронутые адреса
        refetched = session_db.query(LPUAddress).filter(or_(LPUAddress.address_point_id.in_(updated_addresses_before.keys()))).all()
        filled = [a for a in refetched if a.short_name]
        flash(f'Backfill завершён: адресов без short_name исходно={len(missing)}, API вызовов={api_calls}, заполнено теперь={len(filled)}', 'success')
    except Exception as e:
        flash(f'Backfill ошибка: {e}', 'danger')
    finally:
        session_db.close()
    return redirect(url_for('admin_model_list', model_key='doctor'))

@app.route('/admin/model/<model_key>/<int:obj_id>/delete', methods=['POST'])
def admin_model_delete(model_key, obj_id):
    if not _admin_required():
        return redirect(url_for('login'))
    cfg = ADMIN_MODELS.get(model_key)
    if not cfg or not cfg.get('delete'):
        return redirect(url_for('admin_model_list', model_key=model_key))
    Model = cfg['model']
    session_db = get_db_session()
    obj = session_db.query(Model).filter_by(id=obj_id).first()
    if obj:
        if model_key == 'schedule':
            # Вместо удаления строки очищаем расписание, чтобы не ловить UNIQUE ошибки при повторном сохранении.
            try:
                obj.schedule_text = '[]'
                # Ставим updated_at на сейчас
                try:
                    import datetime as _dt
                    obj.updated_at = _dt.datetime.utcnow()
                except Exception:
                    pass
                session_db.commit()
                flash('Расписание сброшено (schedule_text=[])', 'warning')
                try:
                    log_user_action(session_db, session.get('user_id'), 'admin_clear_schedule', f'schedule#{obj_id}', source='web', status='warning')
                except Exception:
                    pass
            except Exception as clr_err:
                session_db.rollback()
                flash(f'Не удалось сбросить расписание: {clr_err}', 'danger')
        else:
            session_db.delete(obj)
            session_db.commit()
            try:
                log_user_action(session_db, session.get('user_id'), 'admin_delete', f'{model_key}#{obj_id}', source='web', status='warning')
            except Exception:
                pass
    session_db.close()
    # После удаления возвращаемся к списку соответствующей модели
    return redirect(url_for('admin_model_list', model_key=model_key))

@app.route('/admin/user/<int:user_id>/make_admin', methods=['POST'])
def make_admin(user_id):
    if 'user_id' not in session or not is_admin(session['user_id']):
        return redirect(url_for('login'))
    session_db = get_db_session()
    profile = session_db.query(UserProfile).filter_by(telegram_user_id=user_id).first()
    if profile:
        profile.is_admin = True
        session_db.commit()
    session_db.close()
    return redirect(url_for('admin_dashboard'))

@app.route('/user/toggle_track/<doctor_id>', methods=['POST'])
def toggle_track(doctor_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    try:
        track = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
        if track:
            track.active = not track.active
            action = 'Возобновлено отслеживание' if track.active else 'Приостановлено отслеживание'
            doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
            doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
            log_user_action(session_db, user_id, action, f'Врач: {doctor_name}', source='web', status='info')
            session_db.commit()
    finally:
        session_db.close()
    return redirect(url_for('user_dashboard'))

def _parse_rule(rule: str):
    # Returns (prefix, start, end) or None
    try:
        parts = rule.strip().split()
        if len(parts) != 2:
            return None
        prefix, times = parts
        if '-' not in times:
            return None
        start, end = times.split('-', 1)
        if len(start) != 5 or len(end) != 5:
            return None
        return prefix.lower(), start, end
    except Exception:
        return None

def _merge_intervals(intervals):
    # intervals: list of (start,end) strings HH:MM
    if not intervals:
        return []
    to_minutes = lambda t: int(t[:2]) * 60 + int(t[3:5])
    intervals_num = sorted([(to_minutes(s), to_minutes(e)) for s, e in intervals])
    merged = []
    for s, e in intervals_num:
        if not merged or s > merged[-1][1]:
            merged.append([s, e])
        else:
            if e > merged[-1][1]:
                merged[-1][1] = e
    # back to strings
    def to_str(m):
        return f"{m//60:02d}:{m%60:02d}"
    return [(to_str(s), to_str(e)) for s, e in merged]

def _merge_rules(rules):
    # group by prefix
    grouped = {}
    for r in rules:
        parsed = _parse_rule(r)
        if not parsed:
            continue
        prefix, s, e = parsed
        grouped.setdefault(prefix, []).append((s, e))
    merged_rules = []
    for prefix, intervals in grouped.items():
        for s, e in _merge_intervals(intervals):
            merged_rules.append(f"{prefix} {s}-{e}")
    # Preserve stable ordering by sorting prefixes alphabetically then time
    return sorted(merged_rules)

WEEKDAY_MAP = {0:'понедельник',1:'вторник',2:'среда',3:'четверг',4:'пятница',5:'суббота',6:'воскресенье'}

def _coverage_for_day(date_obj, rules):
    # returns dict with intervals and coverage classification
    # Build rule intervals for this specific date: match weekday name or exact date yyyy-mm-dd or relative labels
    date_prefixes = set()
    iso_date = date_obj.strftime('%Y-%m-%d')
    date_prefixes.add(iso_date)
    date_prefixes.add(WEEKDAY_MAP[date_obj.weekday()])
    today = dt.date.today()
    if date_obj == today:
        date_prefixes.add('сегодня')
    if date_obj == today + dt.timedelta(days=1):
        date_prefixes.add('завтра')
    # Collect intervals
    day_intervals = []
    for r in rules:
        parsed = _parse_rule(r)
        if not parsed: continue
        prefix,s,e = parsed
        if prefix in date_prefixes:
            day_intervals.append((s,e))
    merged = _merge_intervals(day_intervals)
    return merged  # list of (s,e)

def _classify_coverage(work_intervals, rule_intervals):
    # work_intervals, rule_intervals lists of (s,e)
    if not work_intervals:
        return 'none'
    # Convert to minutes and measure coverage proportion
    to_minutes = lambda t: int(t[:2])*60+int(t[3:5])
    total = 0
    for s,e in work_intervals:
        total += to_minutes(e)-to_minutes(s)
    if total == 0:
        return 'none'
    # build covered segments intersection
    covered = 0
    wi = [(to_minutes(s), to_minutes(e)) for s,e in work_intervals]
    ri = [(to_minutes(s), to_minutes(e)) for s,e in rule_intervals]
    # simple sweep
    for ws,we in wi:
        for rs,re in ri:
            inter_s = max(ws, rs)
            inter_e = min(we, re)
            if inter_s < inter_e:
                covered += (inter_e - inter_s)
    if covered == 0:
        return 'none'
    ratio = covered / total
    if ratio >= 0.999:
        return 'full'
    return 'partial'

def _enrich_schedule_with_coverage(schedule_days, rules):
    # rules expected already merged
    for d in schedule_days:
        # parse date dd.mm -> construct date with current year (approx) by searching in original extraction? We don't have year; skip coverage if ambiguous
        try:
            # Attempt to infer year: assume current year; may be wrong near new year boundary but acceptable
            day, month = d['date'].split('.')
            date_obj = dt.date(dt.date.today().year, int(month), int(day))
            rule_ints = _coverage_for_day(date_obj, rules)
            work_ints = []
            for w in d.get('worktimes', []):
                if '-' in w:
                    ws,we = w.split('-',1)
                    work_ints.append((ws,we))
            coverage = _classify_coverage(work_ints, rule_ints)
            d['coverage'] = coverage
        except Exception:
            d['coverage'] = 'unknown'
    return schedule_days

@app.route('/user/edit_track/<doctor_id>', methods=['GET', 'POST'])
def edit_track(doctor_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    session_db = get_db_session()
    
    try:
        track = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
        
        if not track:
            return redirect(url_for('user_dashboard'))

        if request.method == 'POST':
            if 'rules' in request.form:
                raw_rules = request.form.get('rules') or ''
                new_rules = [r.strip() for r in raw_rules.split(',') if r.strip()]
                
                # Нормализация правил через функцию из rules_parser.py (фиксируем сегодня/завтра)
                try:
                    from rules_parser import parse_user_tracking_input
                    normalized_dict_rules = parse_user_tracking_input(', '.join(new_rules))
                except ImportError as e:
                    flash(f'Ошибка импорта rules_parser.py: {e}', 'error')
                    return redirect(url_for('user_dashboard'))
                except Exception as e:
                    flash(f'Ошибка парсинга правил: {e}', 'error')
                    return redirect(url_for('user_dashboard'))
                
                # Сохраняем правила в JSON формате (как в боте)
                dedup_rules = normalized_dict_rules
                
                changed = dedup_rules != (track.tracking_rules or [])
                if changed:
                    track.tracking_rules = dedup_rules
                    from database import log_user_action
                    # Преобразуем JSON правила в строки для лога
                    rules_str = []
                    for rule in track.tracking_rules:
                        if isinstance(rule, dict):
                            value = rule.get('value', '')
                            time_ranges = rule.get('timeRanges', [])
                            if time_ranges:
                                for tr in time_ranges:
                                    rules_str.append(f"{value}: {tr}" if value else tr)
                            elif value:
                                rules_str.append(value)
                        else:
                            rules_str.append(str(rule))
                    log_user_action(session_db, user_id, 'Изменение правил', f'Врач: {doctor_id}, теперь правил: {len(track.tracking_rules)} => {", ".join(rules_str)}', source='web', status='success')
                    session_db.commit()
                    # Попробовать записаться сразу если включена автозапись и есть подходящий слот
                    if track.auto_booking and track.tracking_rules:
                        try:
                            # простейший поиск ближайшего слота из сохранённого расписания (если есть)
                            sched_rec = session_db.query(DoctorSchedule).filter_by(doctor_api_id=doctor_id).first()
                            if sched_rec:
                                import json, datetime as _dt
                                raw_sched = json.loads(sched_rec.schedule_text)
                                # собрать все слоты ISO -> отфильтровать по правилам
                                all_slots = []
                                for day in raw_sched:
                                    for block in day.get('scheduleBySlot', []):
                                        for sl in block.get('slot', []):
                                            st = sl.get('startTime') or sl.get('start')
                                            if st:
                                                all_slots.append(st[:16])
                                # фильтрация по правилам
                                def slot_matches(slot):
                                    # slot: YYYY-MM-DDTHH:MM
                                    date_part = slot[:10]
                                    time_part = slot[11:16]
                                    weekday_ru = WEEKDAY_MAP[_dt.datetime.strptime(date_part, '%Y-%m-%d').weekday()].lower()
                                    for r in track.tracking_rules:
                                        pr = _parse_rule(r)
                                        if not pr: continue
                                        prefix, rs, re = pr
                                        if prefix == date_part or prefix == weekday_ru or (prefix=='сегодня' and date_part == _dt.date.today().strftime('%Y-%m-%d')) or (prefix=='завтра' and date_part == (_dt.date.today()+_dt.timedelta(days=1)).strftime('%Y-%m-%d')):
                                            if rs <= time_part < re:
                                                return True
                                    return False
                                matched = sorted([s for s in all_slots if slot_matches(s)])
                                if matched:
                                    # Здесь можно дернуть асинхронный бот booking через таск/очередь — пока просто лог
                                    log_user_action(session_db, user_id, 'Автопоиск слота', f'Найден слот {matched[0]} (пока без записи)', source='web', status='info')
                        except Exception as e:
                            flash(f'Автозапись не выполнена: {e}', 'warning')
                            log_user_action(session_db, user_id, 'Автозапись ошибка', f'Врач: {doctor_id} ошибка: {e}', source='web', status='error')
                    flash('Правила обновлены!', 'success')
                else:
                    flash('Изменений нет', 'info')
            return redirect(url_for('user_dashboard'))
        
        # GET request: pre-load rules before session closes
        # Преобразуем правила из базы (могут быть строками или JSON объектами) в строки для веба
        raw_rules = track.tracking_rules if track.tracking_rules else []
        rules = []
        for rule in raw_rules:
            if isinstance(rule, dict):
                # JSON объект из бота - преобразуем в строку
                rtype = rule.get('type', '')
                value = rule.get('value', '')
                time_ranges = rule.get('timeRanges', [])
                
                if time_ranges:
                    for tr in time_ranges:
                        if value:
                            rules.append(f"{value}: {tr}")
                        else:
                            rules.append(tr)
                else:
                    if value:
                        rules.append(value)
            else:
                # Уже строка - оставляем как есть
                rules.append(str(rule))
        doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
        # Schedule preview
        schedule_days = []
        sched = session_db.query(DoctorSchedule).filter_by(doctor_api_id=doctor_id).first()
        refresh = request.args.get('refresh') == '1'
        if refresh and doctor:
            try:
                api_resp = get_available_resource_schedule_info(
                    user_id, int(doctor.doctor_api_id), int(doctor.complex_resource_id)
                )
                if api_resp and api_resp.get('payload') and api_resp['payload'].get('scheduleOfDay'):
                    payload_part = api_resp['payload']['scheduleOfDay']
                    if isinstance(payload_part, list):
                        text = json.dumps(payload_part, ensure_ascii=False)
                        if not sched:
                            sched = DoctorSchedule(doctor_api_id=doctor_id, schedule_text=text)
                            session_db.add(sched)
                        else:
                            sched.schedule_text = text
                        sched.updated_at = dt.datetime.utcnow()
                        session_db.commit()
                        flash('Расписание обновлено из API', 'success')
            except Exception as e:
                flash(f'Не удалось обновить расписание: {e}', 'warning')
        if sched:
            try:
                import json, datetime as _dt
                raw = json.loads(sched.schedule_text)
                for day in raw[:21]:
                    day_date_str = day.get('date') or day.get('scheduleDate')
                    if not day_date_str:
                        continue
                    parsed_date = None
                    for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
                        try:
                            parsed_date = _dt.datetime.strptime(day_date_str[:10], fmt).date()
                            break
                        except Exception:
                            continue
                    if not parsed_date:
                        continue
                    slots_collected = []
                    for block in day.get('scheduleBySlot', []):
                        for sl in block.get('slot', []):
                            st_iso = sl.get('startTime') or sl.get('start')
                            if st_iso and len(st_iso) >= 16:
                                slots_collected.append(st_iso.replace('T',' ')[:16])
                    schedule_days.append({
                        'date': parsed_date.strftime('%d.%m'),
                        'weekday': ['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][parsed_date.weekday()],
                        'slots': sorted(slots_collected),
                        'is_today': parsed_date == _dt.date.today(),
                    })
            except Exception:
                pass
        # coverage enrichment
        merged_rules = _merge_rules(rules)
        schedule_days = _enrich_schedule_with_coverage(schedule_days, merged_rules)
        return render_template('edit_track.html', track=track, doctor=doctor, rules=rules, schedule_days=schedule_days, sched=sched)
    finally:
        session_db.close()

@app.route('/user/delete_track/<doctor_id>', methods=['POST'])
def delete_track(doctor_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    try:
        track = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
        if track:
            doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
            doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
            log_user_action(session_db, user_id, 'Удаление отслеживания', f'Врач: {doctor_name}', source='web', status='warning')
            session_db.delete(track)
            session_db.commit()
    finally:
        session_db.close()
    return redirect(url_for('user_dashboard'))

@app.route('/user/delete_favorite/<doctor_id>', methods=['POST'])
def delete_favorite(doctor_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    try:
        fav = session_db.query(UserFavoriteDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
        if fav:
            doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
            doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
            log_user_action(session_db, user_id, 'Удаление из избранного', f'Врач: {doctor_name}', source='web', status='warning')
            session_db.delete(fav)
            session_db.commit()
    finally:
        session_db.close()
    return redirect(url_for('user_dashboard'))

@app.route('/user/add_track', methods=['GET', 'POST'])
def add_track():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        doctor_id = request.form.get('doctor_id')
        rules = request.form.get('rules')
        user_id = session['user_id']
        session_db = get_db_session()
        try:
            # Проверить, не существует ли уже
            existing = session_db.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
            rule_list = [r.strip() for r in (rules.split(',') if rules else []) if r.strip()]
            # merge overlaps
            rule_list = _merge_rules(rule_list)
            # deduplicate
            seen = set()
            dedup_rules = []
            for r in rule_list:
                if r not in seen:
                    seen.add(r)
                    dedup_rules.append(r)
            if not existing:
                track = UserTrackedDoctor(
                    telegram_user_id=user_id,
                    doctor_api_id=doctor_id,
                    tracking_rules=dedup_rules,
                    active=True
                )
                session_db.add(track)
                doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
                doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
                log_user_action(session_db, user_id, 'Добавлено отслеживание', f'Врач: {doctor_name}', source='web', status='success')
                if dedup_rules:
                    log_user_action(session_db, user_id, 'Создание правил', f'Врач: {doctor_name}, правил: {len(dedup_rules)}', source='web', status='success')
                session_db.commit()
                flash('Врач добавлен в отслеживание!', 'success')
            else:
                # Добавляем новые правила к существующим без дублей
                current = existing.tracking_rules or []
                added = 0
                for r in dedup_rules:
                    if r not in current:
                        current.append(r)
                        added += 1
                if added:
                    existing.tracking_rules = current
                    try:
                        from sqlalchemy.orm.attributes import flag_modified
                        flag_modified(existing, 'tracking_rules')
                    except Exception:
                        pass
                    doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
                    doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
                    log_user_action(session_db, user_id, 'Дополнение правил', f'Врач: {doctor_name}, добавлено: {added}, всего: {len(current)}', source='web', status='success')
                    session_db.commit()
                    flash(f'Добавлено новых правил: {added}', 'success')
                else:
                    flash('Новых правил нет (всё уже есть)', 'info')
        finally:
            session_db.close()
        return redirect(url_for('user_dashboard'))
    
    session_db = get_db_session()
    doctors = session_db.query(DoctorInfo).all()
    # Если выбран doctor_id в GET (например при переключении в форме), показать расписание
    preview_doctor_id = request.args.get('doctor_id')
    schedule_days = []
    refresh = request.args.get('refresh') == '1'
    sched = None
    if preview_doctor_id:
        sched = session_db.query(DoctorSchedule).filter_by(doctor_api_id=preview_doctor_id).first()
        # optional refresh
        if refresh:
            doc_obj = session_db.query(DoctorInfo).filter_by(doctor_api_id=preview_doctor_id).first()
            if doc_obj:
                try:
                    api_resp = get_available_resource_schedule_info(
                        session['user_id'], int(doc_obj.doctor_api_id), int(doc_obj.complex_resource_id)
                    )
                    if api_resp and api_resp.get('payload') and api_resp['payload'].get('scheduleOfDay'):
                        payload_part = api_resp['payload']['scheduleOfDay']
                        if isinstance(payload_part, list):
                            text = json.dumps(payload_part, ensure_ascii=False)
                            if not sched:
                                sched = DoctorSchedule(doctor_api_id=preview_doctor_id, schedule_text=text)
                                session_db.add(sched)
                            else:
                                sched.schedule_text = text
                            sched.updated_at = dt.datetime.utcnow()
                            session_db.commit()
                            flash('Расписание обновлено из API', 'success')
                except Exception as e:
                    flash(f'Не удалось обновить расписание: {e}', 'warning')
        if sched:
            try:
                import json, datetime as _dt
                raw = json.loads(sched.schedule_text)
                for day in raw[:21]:  # ограничим 3 недели
                    day_date_str = day.get('date') or day.get('scheduleDate')
                    if not day_date_str:
                        continue
                    # пытаемся разные форматы
                    parsed_date = None
                    for fmt in ('%Y-%m-%d', '%d.%m.%Y'):
                        try:
                            parsed_date = _dt.datetime.strptime(day_date_str[:10], fmt).date()
                            break
                        except Exception:
                            continue
                    if not parsed_date:
                        continue
                    slots_collected = []
                    for block in day.get('scheduleBySlot', []):
                        for sl in block.get('slot', []):
                            st_iso = sl.get('startTime') or sl.get('start')
                            if st_iso and len(st_iso) >= 16:
                                slots_collected.append(st_iso.replace('T',' ')[:16])
                    schedule_days.append({
                        'date': parsed_date.strftime('%d.%m'),
                        'weekday': ['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][parsed_date.weekday()],
                        'slots': sorted(slots_collected),
                        'is_today': parsed_date == _dt.date.today(),
                    })
            except Exception:
                pass
    # No coverage on add page (rules not yet persisted) – could compute later if needed
    session_db.close()
    return render_template('add_track.html', doctors=doctors, schedule_days=schedule_days, preview_doctor_id=preview_doctor_id, sched=sched)

@app.route('/user/logs')
def user_logs():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    q = session_db.query(UserLog).filter_by(telegram_user_id=user_id).order_by(UserLog.timestamp.desc())
    # параметры
    all_param = request.args.get('all') == '1'
    page_size = 100
    page = request.args.get('page', 1, type=int)
    if page < 1:
        page = 1
    total_count = q.count()
    total_pages = (total_count + page_size - 1) // page_size if total_count else 1
    if all_param:
        logs = q.all()
        total_pages = 1
        page = 1
    else:
        if page > total_pages:
            page = total_pages
        logs = q.offset((page - 1) * page_size).limit(page_size).all()
    # Обогащение: подставим ФИО врача в details вместо голого ID (для действий правил / бронирования)
    try:
        # Собираем все упомянутые doctor_api_id шаблоном 'Доктор <digits>'
        import re
        doctor_ids = set()
        for lg in logs:
            if lg.details:
                for m in re.finditer(r'Доктор\s+(\d{6,})', lg.details):
                    doctor_ids.add(m.group(1))
        if doctor_ids:
            docs = session_db.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.in_(list(doctor_ids))).all()
            doc_map = {d.doctor_api_id: d.name for d in docs if d.name}
        else:
            doc_map = {}
        # Форматированная дата и подстановка имён
        months_rus = {1:'янв',2:'фев',3:'мар',4:'апр',5:'мая',6:'июн',7:'июл',8:'авг',9:'сен',10:'окт',11:'ноя',12:'дек'}
        msk_tz = dt.timezone(dt.timedelta(hours=3))
        for lg in logs:
            # Красивый формат времени: 27 сен 12:34:56
            try:
                ts = lg.timestamp
                # Преобразуем к UTC (если naive считаем что это UTC), затем в МСК
                if ts.tzinfo is None:
                    ts_utc = ts.replace(tzinfo=dt.timezone.utc)
                else:
                    ts_utc = ts.astimezone(dt.timezone.utc)
                ts_msk = ts_utc.astimezone(msk_tz)
                lg.pretty_time = f"{ts_msk.day} {months_rus.get(ts_msk.month, ts_msk.month)} {ts_msk.strftime('%H:%M:%S')}"
            except Exception:
                lg.pretty_time = ''
            if lg.details and 'Доктор ' in lg.details:
                def _sub(match):
                    did = match.group(1)
                    nm = doc_map.get(did)
                    return f"Доктор {nm} (ID {did})" if nm else match.group(0)
                lg.details = re.sub(r'Доктор\s+(\d{6,})', _sub, lg.details)
    except Exception:
        for lg in logs:
            lg.pretty_time = ''
    session_db.close()
    return render_template('user_logs.html', logs=logs, page=page, total_pages=total_pages, page_size=page_size, total_count=total_count, show_all=all_param)

@app.route('/user/logs/delete', methods=['POST'])
def delete_user_logs():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    try:
        # Отправка происходит с двумя вариантами: кнопка (name=delete_all) или скрытое поле
        delete_all = request.form.get('delete_all') == '1'
        selected = request.form.getlist('log_ids')
        if delete_all:
            # Count first
            count = session_db.query(UserLog).filter_by(telegram_user_id=user_id).count()
            session_db.query(UserLog).filter_by(telegram_user_id=user_id).delete(synchronize_session=False)
            session_db.commit()
            # Log deletion action (will create one new log)
            log_user_action(session_db, user_id, 'Удаление логов', f'Удалено: {count} (все)', source='web', status='warning')
            flash(f'Удалено логов: {count}', 'success')
        else:
            if not selected:
                flash('Не выбрано ни одного лога', 'warning')
            else:
                # Only delete those belonging to user
                ids_int = []
                for v in selected:
                    try:
                        ids_int.append(int(v))
                    except Exception:
                        continue
                if ids_int:
                    count = session_db.query(UserLog).filter(UserLog.telegram_user_id==user_id, UserLog.id.in_(ids_int)).count()
                    session_db.query(UserLog).filter(UserLog.telegram_user_id==user_id, UserLog.id.in_(ids_int)).delete(synchronize_session=False)
                    session_db.commit()
                    log_user_action(session_db, user_id, 'Удаление логов', f'Удалено: {count} (выбранные)', source='web', status='warning')
                    flash(f'Удалено выбранных логов: {count}', 'success')
                else:
                    flash('Не удалось распознать выбранные элементы', 'danger')
    finally:
        session_db.close()
    return redirect(url_for('user_logs'))

@app.route('/admin/logs')
def admin_logs():
    if 'user_id' not in session or not is_admin(session['user_id']):
        return redirect(url_for('login'))
    session_db = get_db_session()
    # Фильтрация: по умолчанию показываем логи текущего админа, если не указан параметр user.
    user_param = request.args.get('user')
    show_all = request.args.get('all') == '1'
    selected_user_id = None
    query = session_db.query(UserLog)
    if not show_all:
        if user_param:
            try:
                selected_user_id = int(user_param)
            except ValueError:
                selected_user_id = session['user_id']
        else:
            selected_user_id = session['user_id']
        query = query.filter(UserLog.telegram_user_id == selected_user_id)
    logs = query.order_by(UserLog.timestamp.desc()).limit(200).all()
    # Подготовим pretty_time в МСК так же как и для user_logs
    months_rus = {1:'янв',2:'фев',3:'мар',4:'апр',5:'мая',6:'июн',7:'июл',8:'авг',9:'сен',10:'окт',11:'ноя',12:'дек'}
    msk_tz = dt.timezone(dt.timedelta(hours=3))
    for lg in logs:
        try:
            ts = lg.timestamp
            if ts.tzinfo is None:
                ts_utc = ts.replace(tzinfo=dt.timezone.utc)
            else:
                ts_utc = ts.astimezone(dt.timezone.utc)
            ts_msk = ts_utc.astimezone(msk_tz)
            lg.pretty_time = f"{ts_msk.day} {months_rus.get(ts_msk.month, ts_msk.month)} {ts_msk.strftime('%H:%M:%S')}"
        except Exception:
            lg.pretty_time = ''
    session_db.close()
    return render_template('admin_logs.html', logs=logs, selected_user_id=selected_user_id, show_all=show_all)

@app.route('/user/make_self_admin', methods=['POST'])
def make_self_admin():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    session_db = get_db_session()
    profile = session_db.query(UserProfile).filter_by(telegram_user_id=user_id).first()
    if profile and not profile.is_admin:
        # Check if any admin exists
        admin_count = session_db.query(UserProfile).filter_by(is_admin=True).count()
        if admin_count == 0:
            profile.is_admin = True
            session_db.commit()
            flash('Вы стали админом!')
        else:
            flash('Админ уже существует.')
    session_db.close()
    return redirect(url_for('user_dashboard'))

@app.route('/user/set_password', methods=['GET', 'POST'])
def set_password():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    if request.method == 'POST':
        password = request.form.get('password')
        session_db = get_db_session()
        profile = session_db.query(UserProfile).filter_by(telegram_user_id=user_id).first()
        if profile:
            profile.password = password
            session_db.commit()
            flash('Пароль установлен!')
        session_db.close()
        return redirect(url_for('user_dashboard'))
    return render_template('set_password.html')

@app.route('/user/add_favorite', methods=['GET', 'POST'])
def add_favorite():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        doctor_id = request.form.get('doctor_id')
        user_id = session['user_id']
        session_db = get_db_session()
        try:
            existing = session_db.query(UserFavoriteDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=doctor_id).first()
            if not existing:
                fav = UserFavoriteDoctor(telegram_user_id=user_id, doctor_api_id=doctor_id)
                session_db.add(fav)
                doctor = session_db.query(DoctorInfo).filter_by(doctor_api_id=doctor_id).first()
                doctor_name = doctor.name if doctor else f'ID: {doctor_id}'
                log_user_action(session_db, user_id, 'Добавление в избранное', f'Врач: {doctor_name}', source='web', status='success')
                session_db.commit()
                flash('Врач добавлен в избранное!', 'success')
            else:
                flash('Врач уже в избранном!', 'warning')
        finally:
            session_db.close()
        return redirect(url_for('user_dashboard'))
    
    session_db = get_db_session()
    doctors = session_db.query(DoctorInfo).all()
    session_db.close()
    return render_template('add_favorite.html', doctors=doctors)

@app.route('/user/bulk_track', methods=['GET','POST'])
def bulk_track():
    """Массовое добавление отслеживания.
    Пользователь вводит список doctor_id (через пробел, запятую или новую строку) и общие правила.
    Использование: позволяет сразу подписаться на несколько кабинетов (например ЭКГ) или несколько ресурсов СМАД.
    """
    import re  # вынесено из ветки POST, чтобы не было UnboundLocalError при использовании re в GET
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    if request.method == 'POST':
        ids_raw = request.form.get('doctor_ids','')
        stop_on_first = request.form.get('stop_on_first') == '1'
        auto_booking_all = request.form.get('auto_booking') == '1'
        rules_raw = request.form.get('rules','')
        # Парсинг doctor_ids — допускаем разделители: пробел, запятая, новая строка
        doc_ids = []
        for tok in re.split(r'[\s,;]+', ids_raw.strip()):
            if tok:
                doc_ids.append(tok)
        # Правила аналогично add_track: строка с правилами через запятую
        raw_rules_tokens = [r.strip() for r in (rules_raw.split(',') if rules_raw else []) if r.strip()]
        # Дополнительно разрешаем ввод без запятых: пробельное разделение если нет запятых вообще
        if rules_raw and ',' not in rules_raw and '\n' not in rules_raw:
            # например: "понедельник вторник 10:00-12:00" -> разобьём и сохраним
            extra = [t for t in rules_raw.split() if t]
            # если это дало больше токенов и исходный список короткий – используем
            if len(extra) > len(raw_rules_tokens):
                raw_rules_tokens = extra
        # Преобразуем одиночные дни недели без интервала в полный день 00:00-23:59
        WEEKDAYS_SIMPLE = {'понедельник','вторник','среда','четверг','пятница','суббота','воскресенье'}
        norm_rules = []
        for r in raw_rules_tokens:
            if ' ' not in r and '-' not in r and r.lower() in WEEKDAYS_SIMPLE:
                norm_rules.append(f"{r.lower()} 00:00-23:59")
            else:
                norm_rules.append(r)
        rule_list = norm_rules
        # merge + dedup
        rule_list = _merge_rules(rule_list)
        seen_rules = set()
        dedup_rules = []
        for r in rule_list:
            if r not in seen_rules:
                seen_rules.add(r)
                dedup_rules.append(r)
        sess = get_db_session()
        added = 0
        updated = 0
        # Генерируем batch_id если выбрано более 1 врача и включены stop_on_first или auto_booking
        import uuid
        batch_id = None
        preliminary_ids = [i for i in doc_ids if i]
        if len(preliminary_ids) > 1 and (stop_on_first or auto_booking_all):
            batch_id = uuid.uuid4().hex
        try:
            from database import UserTrackedDoctor, DoctorInfo
            for did in doc_ids:
                if not did:
                    continue
                track = sess.query(UserTrackedDoctor).filter_by(telegram_user_id=user_id, doctor_api_id=did).first()
                if not track:
                    track = UserTrackedDoctor(telegram_user_id=user_id, doctor_api_id=did, tracking_rules=dedup_rules.copy(), active=True)
                    if auto_booking_all:
                        track.auto_booking = True
                    if batch_id:
                        track.bulk_batch_id = batch_id
                        track.stop_after_first = stop_on_first
                    sess.add(track)
                    added += 1
                    # лог
                    try:
                        doc = sess.query(DoctorInfo).filter_by(doctor_api_id=did).first()
                        dname = doc.name if doc else did
                        log_user_action(sess, user_id, 'Массовое отслеживание добавлено', f'Врач {dname}', source='web', status='success')
                    except Exception:
                        pass
                else:
                    # дополняем правила
                    current = track.tracking_rules or []
                    add_cnt = 0
                    for r in dedup_rules:
                        if r not in current:
                            current.append(r)
                            add_cnt += 1
                    if add_cnt:
                        track.tracking_rules = current
                        try:
                            from sqlalchemy.orm.attributes import flag_modified
                            flag_modified(track, 'tracking_rules')
                        except Exception:
                            pass
                        updated += 1
                    # включаем автозапись для существующего если попросили
                    if batch_id:
                        # Подтягиваем batch параметры для существующего отслеживания если ещё не выставлены
                        if not track.bulk_batch_id:
                            track.bulk_batch_id = batch_id
                        if stop_on_first:
                            track.stop_after_first = True
                    if auto_booking_all and not track.auto_booking:
                        track.auto_booking = True
                        try:
                            log_user_action(sess, user_id, 'bulk_auto_booking_on', f'doctor={did}', source='web', status='info')
                        except Exception:
                            pass
                        try:
                            doc = sess.query(DoctorInfo).filter_by(doctor_api_id=did).first()
                            dname = doc.name if doc else did
                            log_user_action(sess, user_id, 'Массовое правила дополнены', f'Врач {dname} +{add_cnt}', source='web', status='info')
                        except Exception:
                            pass
            sess.commit()
        finally:
            sess.close()
        if added or updated:
            extra_parts = []
            if stop_on_first:
                extra_parts.append('stop_after_first')
            if auto_booking_all:
                extra_parts.append('auto_booking=on')
            extra = f" ({', '.join(extra_parts)})" if extra_parts else ''
            flash(f'Добавлено: {added}, обновлено: {updated}{extra}', 'success')
        else:
            flash('Нет изменений (все уже отслеживаются с этими правилами)', 'info')
        return redirect(url_for('user_dashboard'))
    # GET – показываем форму
    sess = get_db_session()
    try:
        from database import LPUAddress, DoctorInfo
        doctors_raw = (
            sess.query(DoctorInfo)
            .order_by(DoctorInfo.ar_speciality_name, DoctorInfo.name)
            .all()
        )
        # Собираем адреса (full + short_name) по address_point_id (если есть)
        addr_map = {}
        ap_ids = [d.address_point_id for d in doctors_raw if d.address_point_id]
        if ap_ids:
            for addr in sess.query(LPUAddress).filter(LPUAddress.address_point_id.in_(ap_ids)).all():
                addr_map[addr.address_point_id] = {
                    'full': addr.address,
                    'short': addr.short_name or ''
                }
        # Обогащаем временным атрибутом resolved_address + собираем специальности
        specialties = {}
        enriched_doctors = []
        # эквивалентные группы для отображения (например 69 и 602)
        equiv_map = {
            '69': '69|602',  # ключ группы
            '602': '69|602',
        }
        group_labels = {
            '69|602': 'Врач общей практики / Терапевт',
        }
        for d in doctors_raw:
            addr_info = addr_map.get(d.address_point_id) if d.address_point_id else None
            full_addr = (addr_info or {}).get('full') or ''
            short_addr = (addr_info or {}).get('short') or ''
            # resolved_address оставим как полный, если нужен где-то ещё
            setattr(d, 'resolved_address', full_addr)
            raw_code = d.ar_speciality_id or '—'
            raw_name = d.ar_speciality_name or 'Без специальности'
            group_key = equiv_map.get(raw_code, raw_code)
            # Имя группы: если есть в group_labels – берем его, иначе оригинальное имя
            group_name = group_labels.get(group_key, raw_name)
            sp = specialties.setdefault(group_key, {"code": group_key, "name": group_name, "count": 0, "codes": set()})
            sp["count"] += 1
            sp['codes'].add(raw_code)
            enriched_doctors.append({
                'id': d.doctor_api_id,
                'name': d.name,
                'spec_code': group_key,  # фронт будет фильтровать по групповому ключу
                'spec_name': group_name,
                'address_point_id': d.address_point_id,
                'address': short_addr or full_addr,  # используем короткое имя если есть
                'full_address': full_addr,
                'raw_spec_code': raw_code,
                'raw_spec_name': raw_name,
            })
        # Преобразуем множество codes в отсортированный список для сериализации
        for sp in specialties.values():
            sp['codes'] = sorted(sp['codes'])
        specialties_list = sorted(specialties.values(), key=lambda x: x['name'])
        if not enriched_doctors:
            app.logger.warning("bulk_track: no doctors found (doctor_info table empty or filter logic issue)")
        return render_template('bulk_track.html', doctors_json=enriched_doctors, specialties=specialties_list, total_doctors=len(enriched_doctors))
    finally:
        sess.close()


if __name__ == '__main__':
    Base.metadata.create_all(bind=engine)
    app.run(host='0.0.0.0', port=8000, debug=True)