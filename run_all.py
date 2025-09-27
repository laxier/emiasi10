import asyncio
import threading
from bot import main as bot_main
from web_app import app
from database import init_db, engine, Base
from sqlalchemy import inspect

def run_web():
    import os
    port = int(os.environ.get('PORT', 80))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

if __name__ == '__main__':
    # Инициализация БД (создание недостающих таблиц, включая service_shift_tasks)
    try:
        init_db()
        insp = inspect(engine)
        print("[DB] Tables:", insp.get_table_names())
        if 'service_shift_tasks' not in insp.get_table_names():
            print("[DB][WARN] service_shift_tasks still missing after init_db()")
    except Exception as e:
        print(f"[DB][ERROR] init_db failed: {e}")

    # Запуск веб в отдельном потоке
    web_thread = threading.Thread(target=run_web, daemon=True)
    web_thread.start()

    # Запуск бота в основном потоке
    asyncio.run(bot_main())