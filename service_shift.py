"""Generic service shift (blood test / ECG) utilities.

Позволяет переносить существующую запись (appointmentId) на самый ранний подходящий
слот среди ресурсов в заданном ЛПУ (по имени), ориентируясь на окна времени.

Минимальная интеграция: использует существующие токены и профиль пользователя.
API реализовано в стиле standalone (похоже на shift_blood), но без отдельного
sqlite – всё через основную БД.

Использование (пример):

    from service_shift import shift_service_appointment, TimeWindow
    result = shift_service_appointment(
        user_id=123,
        appointment_id=665012345678,
        target_lpu_name="ГП 62 Ф 1 (ГП 71)",
        allowed_windows=[TimeWindow("10:00","12:00")],
        forbidden_windows=[],
        timeout_sec=60,
        poll_interval=10,
        service_label="blood"
    )

Возвращает dict с ключами status ('shifted' | 'not_found' | 'error') и доп. полями.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime
from typing import List, Optional, Tuple, Dict, Any
import time
import requests

from emias_api import refresh_emias_token
from database import get_db_session, get_tokens, get_profile

BASE_URL = "https://emias.info/api-eip/v3/saOrchestrator"
URL_GET_LI = f"{BASE_URL}/getDoctorsInfoForLI"
URL_GET_SCHED = f"{BASE_URL}/getAvailableResourceScheduleInfo"
URL_SHIFT = f"{BASE_URL}/shiftAppointment"


@dataclass
class TimeWindow:
    start: dtime
    end: dtime

    @classmethod
    def parse(cls, a: str, b: str) -> "TimeWindow":
        def _p(x: str):
            hh, mm = x.split(":")
            return dtime(int(hh), int(mm))
        return cls(_p(a), _p(b))


def _make_headers(token: str) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://emias.info",
        "X-Requested-With": "XMLHttpRequest",
        "X-App": "portal",
        "EI-Token": token,
    }


def _api_post(url: str, headers: Dict[str, str], body: Dict[str, Any], timeout: int = 25) -> requests.Response:
    return requests.post(url, headers=headers, json=body, timeout=timeout)


def _get_valid_token(user_id: int) -> Optional[str]:
    sess = get_db_session()
    try:
        tokens = get_tokens(sess, user_id)
        if not tokens:
            return None
        access, _, expires_at = tokens
        if datetime.utcnow() >= expires_at:
            return refresh_emias_token(user_id, source='system')
        return access
    finally:
        sess.close()


def _fetch_li(user_id: int, token: str, appointment_id: int, profile) -> Dict[str, Any]:
    body = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "assignment": {  # Широкий период – месяц вперёд
            "moId": 0,  # 0 = не фильтруем по moId (оставим всё, потом отфильтруем по имени ЛПУ)
            "samplingTypeId": 1,
            "period": {
                "dateFrom": datetime.utcnow().strftime('%Y-%m-%d'),
                "dateTo": (datetime.utcnow().replace(day=28)).strftime('%Y-%m-%d'),
            },
        },
        "appointmentId": str(appointment_id),
    }
    r = _api_post(URL_GET_LI, _make_headers(token), body)
    if r.status_code == 401:
        raise requests.HTTPError("401", response=r)
    r.raise_for_status()
    data = r.json()
    return data.get("payload") or data


def _iter_resources(li_payload: Dict[str, Any], target_lpu_name: str):
    for lpu in li_payload.get("doctorsInfo", []):
        if lpu.get("lpuShortName") != target_lpu_name:
            continue
        for ar in lpu.get("availableResources", []):
            ar_id = int(ar["id"])
            for comp in ar.get("complexResource", []):
                cr_id = int(comp["id"])
                cabinet = comp.get("name") or comp.get("room", {}).get("number", "")
                yield ar_id, cr_id, str(cabinet)


def _fetch_sched(token: str, profile, appointment_id: int, ar_id: int, cr_id: int):
    body = {
        "omsNumber": profile.oms_number,
        "birthDate": profile.birth_date,
        "availableResourceId": ar_id,
        "complexResourceId": cr_id,
        "period": {
            "dateFrom": datetime.utcnow().strftime('%Y-%m-%d'),
            "dateTo": (datetime.utcnow().replace(day=28)).strftime('%Y-%m-%d'),
        },
        "appointmentId": int(appointment_id),
    }
    r = _api_post(URL_GET_SCHED, _make_headers(token), body)
    if r.status_code in (400,):  # бизнес-ошибка – трактуем как нет слотов
        return {}
    if r.status_code == 401:
        raise requests.HTTPError("401", response=r)
    r.raise_for_status()
    data = r.json()
    return data.get("payload") or data


def _slot_passes(tw: List[TimeWindow], fw: List[TimeWindow], start: datetime, end: datetime) -> bool:
    def _in_any(wl: List[TimeWindow], t: datetime) -> bool:
        return any(w.start <= t.time() < w.end for w in wl)
    if tw and not _in_any(tw, start):
        return False
    if any(w.start <= start.time() < w.end for w in fw):
        return False
    return True


def _pick_earliest(schedule_payload: Dict[str, Any], allowed: List[TimeWindow], forbidden: List[TimeWindow]) -> Optional[Tuple[datetime, datetime]]:
    if not schedule_payload:
        return None
    best = None
    for day in schedule_payload.get("scheduleOfDay", []):
        for blk in day.get("scheduleBySlot", []):
            for s in blk.get("slot", []):
                try:
                    st = datetime.fromisoformat(s["startTime"])  # naive ISO ok (server local)
                    en = datetime.fromisoformat(s["endTime"])
                except Exception:
                    continue
                if not _slot_passes(allowed, forbidden, st, en):
                    continue
                if best is None or st < best[0]:
                    best = (st, en)
    return best


def shift_service_appointment(
    user_id: int,
    appointment_id: int,
    target_lpu_name: str,
    allowed_windows: List[TimeWindow],
    forbidden_windows: List[TimeWindow],
    timeout_sec: int = 300,
    poll_interval: int = 15,
    service_label: str = "generic"
) -> Dict[str, Any]:
    """Основной цикл поиска и переноса. Блокирующий.

    Возвращает dict:
        {"status": "shifted", "start": ..., "end": ..., "cabinet": ...}
        или {"status": "not_found"} / {"status": "error", "error": str}
    """
    sess = get_db_session()
    profile = get_profile(sess, user_id)
    sess.close()
    if not profile:
        return {"status": "error", "error": "profile_not_found"}

    token = _get_valid_token(user_id)
    if not token:
        return {"status": "error", "error": "no_token"}

    t0 = time.time()
    while True:
        try:
            li = _fetch_li(user_id, token, appointment_id, profile)
            best_global = None  # (ar, cr, cab, st, en)
            for ar_id, cr_id, cab in _iter_resources(li, target_lpu_name):
                sched = _fetch_sched(token, profile, appointment_id, ar_id, cr_id)
                slot = _pick_earliest(sched, allowed_windows, forbidden_windows)
                if not slot:
                    continue
                st, en = slot
                if best_global is None or st < best_global[3]:
                    best_global = (ar_id, cr_id, cab, st, en)
            if best_global:
                ar_id, cr_id, cab, st, en = best_global
                body = {
                    "omsNumber": profile.oms_number,
                    "birthDate": profile.birth_date,
                    "availableResourceId": ar_id,
                    "complexResourceId": cr_id,
                    "startTime": st.isoformat(),
                    "endTime": en.isoformat(),
                    "appointmentId": int(appointment_id),
                }
                r = _api_post(URL_SHIFT, _make_headers(token), body)
                r.raise_for_status()
                return {"status": "shifted", "cabinet": cab, "start": st.isoformat(), "end": en.isoformat(), "service": service_label}
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                token = refresh_emias_token(user_id, source='system')
                if not token:
                    return {"status": "error", "error": "refresh_failed"}
                continue
            return {"status": "error", "error": f"http_{getattr(e.response,'status_code',None)}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

        if time.time() - t0 > timeout_sec:
            return {"status": "not_found"}
        time.sleep(poll_interval)


__all__ = [
    "TimeWindow",
    "shift_service_appointment"
]
