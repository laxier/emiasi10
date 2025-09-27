"""One-off migration script to move service / cabinet entries from doctor_info into service_resources.
Run:
    PYTHONPATH=. python migrate_services.py

Logic:
- Detect rows in doctor_info whose ar_speciality_id is in SERVICE_SPECIALITY_CODES or name contains pattern 'Кабинет_' / 'СМАД' / 'ЭКГ' / 'Рентген'
- For each such row create (or update) ServiceResource record.
- Optionally delete original doctor_info rows (leave if --keep flag passed).
"""
from database import (
    get_db_session,
    DoctorInfo,
    ServiceResource,
    Specialty,
    SERVICE_SPECIALITY_CODES,
    save_or_update_service_resource
)
import argparse
import re

PATTERNS = [r"КАБИНЕТ", r"СМАД", r"ЭКГ", r"РЕНТГЕН"]


def is_service_candidate(doc: DoctorInfo) -> bool:
    if doc.ar_speciality_id and str(doc.ar_speciality_id) in SERVICE_SPECIALITY_CODES:
        return True
    up = (doc.name or '').upper()
    return any(p in up for p in PATTERNS)


def migrate(keep: bool = False, dry_run: bool = False, purge: bool = False):
    sess = get_db_session()
    created = 0
    updated = 0
    deleted = 0
    docs = sess.query(DoctorInfo).all()
    for d in docs:
        if not is_service_candidate(d):
            continue
        # Build a pseudo doctor_data dict to reuse save_or_update_service_resource
        data = {
            'id': d.doctor_api_id,
            'name': d.name,
            'complexResource': [{'id': d.complex_resource_id}] if d.complex_resource_id else [],
            'arSpecialityId': d.ar_speciality_id,
            'arSpecialityName': d.ar_speciality_name,
        }
        existing = sess.query(ServiceResource).filter_by(resource_api_id=str(d.doctor_api_id)).first()
        if not dry_run:
            save_or_update_service_resource(sess, 0, data)  # telegram_user_id=0 (system)
            sess.commit()
        if existing:
            updated += 1
        else:
            created += 1
        if purge and not keep and not dry_run:
            # удаляем связанные расписания сначала
            try:
                from database import DoctorSchedule
                sched = sess.query(DoctorSchedule).filter_by(doctor_api_id=d.doctor_api_id).first()
                if sched:
                    sess.delete(sched)
                    sess.flush()
                sess.delete(d)
                deleted += 1
            except Exception as de:
                print(f"[WARN] Не удалось удалить doctor {d.doctor_api_id}: {de}")
                sess.rollback()
        sess.commit()
    print(f"Service migration summary: created={created} updated={updated} deleted={deleted} keep={keep} purge={purge} dry_run={dry_run}")
    sess.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--keep', action='store_true', help='Do not delete original doctor_info rows')
    ap.add_argument('--dry-run', action='store_true', help='Do not write changes')
    ap.add_argument('--purge', action='store_true', help='Delete original doctor records (and their schedules)')
    args = ap.parse_args()
    migrate(keep=args.keep, dry_run=args.dry_run, purge=args.purge)
