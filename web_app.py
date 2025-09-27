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
    UserDoctorLink,
    ServiceShiftTask,
    ServiceResource,
    SERVICE_SPECIALITY_CODES
)
from emias_api import get_whoami, get_assignments_referrals_info, get_available_resource_schedule_info
import json, datetime as dt
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'default_secret_key')

def format_tracking_rules(rules):
    """Преобразует правила отслеживания в человекочитаемый формат"""
    if not rules:
        return 'Нет'
    
    formatted = []
    for rule in rules:
        if isinstance(rule, dict):
            # JSON объект из бота
            value = rule.get('value', '')
            time_ranges = rule.get('timeRanges', [])
            
            if time_ranges:
                for tr in time_ranges:
                    formatted.append(f"{value}: {tr}")
            else:
                formatted.append(value)
        else:
            # Строковый формат
            formatted.append(str(rule))
    
    return ', '.join(formatted)

# Добавляем функцию в контекст шаблонов
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
        # Аутентификация по username (telegram_user_id) и password
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
    shift_tasks = session_db.query(ServiceShiftTask).filter_by(telegram_user_id=user_id).all()
    favorites = session_db.query(UserFavoriteDoctor).filter_by(telegram_user_id=user_id).all()
    
    # Получить информацию о врачах
    tracked_ids = [t.doctor_api_id for t in tracked]
    favorite_ids = [f.doctor_api_id for f in favorites]
    all_ids = set(tracked_ids + favorite_ids)
    doctors = session_db.query(DoctorInfo).filter(DoctorInfo.doctor_api_id.in_(all_ids)).all()
    doctor_dict = {d.doctor_api_id: d for d in doctors}
    
    session_db.close()
    token_status = None
    remaining_seconds = None
    last_token_update = None
    if tokens and getattr(tokens, 'expires_at', None):
        now = dt.datetime.utcnow()
        delta = (tokens.expires_at - now).total_seconds()
        remaining_seconds = int(delta)
        if delta <= 0:
            token_status = 'expired'
        elif delta < 900:
            token_status = 'soon'
        else:
            token_status = 'ok'
        try:
            last_token_update = tokens.expires_at - dt.timedelta(seconds=3600)
        except Exception:
            pass
    return render_template('user_dashboard.html', profile=profile, tokens=tokens, tracked=tracked, favorites=favorites, doctor_dict=doctor_dict, token_status=token_status, remaining_seconds=remaining_seconds, last_token_update=last_token_update, shift_tasks=shift_tasks)

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

@app.route('/admin')
def admin_dashboard():
    if 'user_id' not in session or not is_admin(session['user_id']):
        return redirect(url_for('login'))
    session_db = get_db_session()
    users = session_db.query(UserProfile).all()
    doctors = session_db.query(DoctorInfo).all()
    service_count = session_db.query(ServiceResource).count()
    session_db.close()
    return render_template('admin_dashboard.html', users=users, doctors=doctors, models=ADMIN_MODELS, service_count=service_count, SERVICE_SPECIALITY_CODES=SERVICE_SPECIALITY_CODES)


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
        elif action == 'set_service_policy':
            try:
                policy = int(request.form.get('policy', 1))
            except Exception:
                policy = 1
            codes = list(SERVICE_SPECIALITY_CODES)
            if not codes:
                flash('SERVICE_SPECIALITY_CODES пуст', 'warning')
            else:
                updated = sess.query(Specialty).filter(Specialty.code.in_(codes)).update({'referral_policy': policy}, synchronize_session=False)
                sess.commit()
                flash(f'Обновлено сервисных спец: {updated} policy={policy}', 'success')
                try:
                    log_user_action(sess, session.get('user_id'), 'admin_bulk', f'set_service_policy policy={policy} updated={updated}', source='web', status='info')
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
        'title': 'Врачи',
        'editable': ['name','ar_speciality_id','ar_speciality_name','complex_resource_id'],
        'create': False,
        'delete': False,
        'order_by': 'doctor_api_id'
    },
    'service_resource': {
        'model': ServiceResource,
        'title': 'Кабинеты / Услуги',
        'editable': ['name','speciality_id','speciality_name','complex_resource_id','resource_type'],
        'create': False,
        'delete': False,
        'order_by': 'resource_api_id'
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
        'title': 'Связки пользователь → спец/услуга',
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
    }
}

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
    query = session_db.query(cfg['model'])
    if cfg.get('order_by'):
        try:
            query = query.order_by(getattr(cfg['model'], cfg['order_by']))
        except Exception:
            pass
    items = query.limit(500).all()
    session_db.close()
    return render_template('admin_model_list.html', cfg=cfg, model_key=model_key, items=items)

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
        for field in cfg['editable']:
            if field in request.form:
                raw = request.form.get(field, '')
                current = getattr(obj, field, None)
                new_val = _coerce_value(current, raw)
                if new_val != current:
                    setattr(obj, field, new_val)
                    changed_fields.append(field)
        session_db.commit()
        try:
            log_user_action(session_db, session.get('user_id'), 'admin_edit', f'{model_key}#{obj_id} поля: {",".join(changed_fields)}', source='web', status='info')
        except Exception:
            pass
    session_db.close()
    return redirect(url_for('admin_model_list', model_key=model_key))
    # For display we just pass object
    session_db.close()
    return render_template('admin_model_form.html', cfg=cfg, model_key=model_key, obj=obj)

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
        session_db.delete(obj)
        session_db.commit()
        try:
            log_user_action(session_db, session.get('user_id'), 'admin_delete', f'{model_key}#{obj_id}', source='web', status='warning')
        except Exception:
            pass
    session_db.close()
    return redirect(url_for('admin_dashboard'))

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
                # Единый лог вместо двух, чтобы на сайте не выглядело как дублирование уведомлений
                rules_info = f", правил: {len(dedup_rules)}" if dedup_rules else ""
                log_user_action(session_db, user_id, 'Добавлено отслеживание', f'Врач: {doctor_name}{rules_info}', source='web', status='success')
                session_db.commit()
                flash('Врач добавлен в отслеживание', 'success')
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


# --- Service Shift Tasks (Blood / ECG) ---

def _parse_time_windows(raw: str):
    windows = []
    for part in (raw or '').split(','):
        part = part.strip()
        if not part or '-' not in part:
            continue
        a, b = part.split('-', 1)
        a = a.strip()
        b = b.strip()
        if len(a) == 5 and len(b) == 5:
            windows.append(f"{a}-{b}")
    return windows

@app.route('/user/service_tasks', methods=['GET','POST'])
def service_tasks():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    sess = get_db_session()
    try:
        if request.method == 'POST':
            task_id = request.form.get('task_id')
            if task_id:  # update / toggle
                task = sess.query(ServiceShiftTask).filter_by(id=task_id, telegram_user_id=user_id).first()
                if task:
                    if 'toggle' in request.form:
                        task.active = not task.active
                        log_user_action(sess, user_id, 'service_task_toggle', f'task={task.id} now={task.active}', source='web', status='info')
                    else:
                        new_type = request.form.get('service_type', task.service_type)
                        task.service_type = new_type
                        task.lpu_substring = request.form.get('lpu_substring', task.lpu_substring)
                        # Обновление правил (если переданы)
                        if 'service_rules' in request.form:
                            raw_rules_str = request.form.get('service_rules','').strip()
                            if raw_rules_str:
                                try:
                                    from rules_parser import parse_user_tracking_input
                                    parsed_rules = parse_user_tracking_input(raw_rules_str)
                                    task.service_rules = parsed_rules or None
                                except Exception as e:
                                    print(f"[service_tasks] rules parse error: {e}")
                            else:
                                task.service_rules = None
                        # Авто-определение referral_required по политике специальности, если выбран код
                        ref_flag_form = bool(request.form.get('referral_required'))
                        if new_type and new_type.isdigit():
                            spec_obj = sess.query(Specialty).filter_by(code=new_type).first()
                            if spec_obj:
                                if spec_obj.referral_policy == 0:  # strict
                                    task.referral_required = True
                                elif spec_obj.referral_policy == 2:  # always allow
                                    task.referral_required = False
                                else:  # fallback – пользовательский чекбокс
                                    task.referral_required = ref_flag_form
                            else:
                                task.referral_required = ref_flag_form
                        else:
                            task.referral_required = ref_flag_form
                        task.allowed_windows = _parse_time_windows(request.form.get('allowed_windows'))
                        task.forbidden_windows = _parse_time_windows(request.form.get('forbidden_windows'))
                        # week days
                        wd_raw = request.form.getlist('week_days')
                        task.week_days = [int(x) for x in wd_raw if x.isdigit()] or None
                        # exact dates
                        ed_raw = request.form.get('exact_dates','')
                        dates = []
                        for part in ed_raw.split(','):
                            p = part.strip()
                            if len(p)==10 and p[4]=='-' and p[7]=='-':
                                dates.append(p)
                        task.exact_dates = dates or None
                        task.mode = request.form.get('mode', task.mode or 'shift')
                        log_user_action(sess, user_id, 'service_task_update', f'task={task.id}', source='web', status='success')
                    sess.commit()
            else:  # create
                # service_type может быть алиасом ('blood','ecg') либо напрямую кодом специальности (напр. 600020)
                service_type = request.form.get('service_type') or 'blood'
                lpu_sub = request.form.get('lpu_substring') or ''
                if lpu_sub:
                    # Определяем referral_required с учётом политики
                    ref_flag_form = bool(request.form.get('referral_required'))
                    ref_required = ref_flag_form
                    if service_type.isdigit():
                        spec_obj = sess.query(Specialty).filter_by(code=service_type).first()
                        if spec_obj:
                            if spec_obj.referral_policy == 0:
                                ref_required = True
                            elif spec_obj.referral_policy == 2:
                                ref_required = False
                    # week days
                    wd_raw = request.form.getlist('week_days')
                    week_days = [int(x) for x in wd_raw if x.isdigit()] or None
                    # exact dates
                    ed_raw = request.form.get('exact_dates','')
                    dates = []
                    for part in ed_raw.split(','):
                        p = part.strip()
                        if len(p)==10 and p[4]=='-' and p[7]=='-':
                            dates.append(p)
                    task = ServiceShiftTask(
                        telegram_user_id=user_id,
                        service_type=service_type,
                        lpu_substring=lpu_sub,
                        referral_required=ref_required,
                        allowed_windows=_parse_time_windows(request.form.get('allowed_windows')),
                        forbidden_windows=_parse_time_windows(request.form.get('forbidden_windows')),
                        week_days=week_days,
                        exact_dates=dates or None,
                        mode=request.form.get('mode','shift'),
                        service_rules=None
                    )
                    # Первичное заполнение правил если переданы
                    raw_rules_str = request.form.get('service_rules','').strip()
                    if raw_rules_str:
                        try:
                            from rules_parser import parse_user_tracking_input
                            parsed_rules = parse_user_tracking_input(raw_rules_str)
                            task.service_rules = parsed_rules or None
                        except Exception as e:
                            print(f"[service_tasks] rules parse error (create): {e}")
                    sess.add(task)
                    sess.commit()
                    log_user_action(sess, user_id, 'service_task_create', f'task={task.id} type={service_type}', source='web', status='success')
        tasks = sess.query(ServiceShiftTask).filter_by(telegram_user_id=user_id).order_by(ServiceShiftTask.id.desc()).all()
        # Предлагаем список LDP специальностей для выбора как service_type (код)
        from database import SERVICE_SPECIALITY_CODES as _SVC_CODES
        ldp_specs = (
            sess.query(Specialty)
            .filter(Specialty.code.in_(list(_SVC_CODES)))
            .order_by(Specialty.code.asc())
            .all()
        )
    finally:
        sess.close()
    # Подготовим словарь политик направления для фронта: code -> referral_policy
    spec_policies = {s.code: (s.referral_policy if s.referral_policy is not None else 1) for s in ldp_specs}
    return render_template('service_tasks.html', tasks=tasks, ldp_specs=ldp_specs, spec_policies=spec_policies)

@app.route('/user/service_tasks/delete/<int:task_id>', methods=['POST'])
def delete_service_task(task_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    user_id = session['user_id']
    sess = get_db_session()
    try:
        task = sess.query(ServiceShiftTask).filter_by(id=task_id, telegram_user_id=user_id).first()
        if task:
            log_user_action(sess, user_id, 'service_task_delete', f'task={task.id}', source='web', status='warning')
            sess.delete(task)
            sess.commit()
    finally:
        sess.close()
    return redirect(url_for('service_tasks'))

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
        for lg in logs:
            # Красивый формат времени: 27 сен 12:34:56
            try:
                ts = lg.timestamp
                lg.pretty_time = f"{ts.day} {months_rus.get(ts.month, ts.month)} {ts.strftime('%H:%M:%S')}"
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


if __name__ == '__main__':
    Base.metadata.create_all(bind=engine)
    app.run(host='0.0.0.0', port=8000, debug=True)