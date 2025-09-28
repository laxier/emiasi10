import asyncio
import threading
import os

# Ensure runtime config module exists before importing modules that `from config import ...`.
# This avoids ModuleNotFoundError inside containers where `config.py` is not present
# (we intentionally keep config.py untracked and rely on env vars in production).
if not os.path.exists('config.py'):
    with open('config.py', 'w') as _f:
        _f.write('import os\n')
        _f.write("TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')\n")
        _f.write("EMIAS_API_BASE_URL = os.environ.get('EMIAS_API_BASE_URL', 'https://emias.info/api-eip/')\n")

from bot import main as bot_main
from web_app import app

def run_web():
    import os
    port = int(os.environ.get('PORT', 80))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

if __name__ == '__main__':
    # Запуск веб в отдельном потоке
    web_thread = threading.Thread(target=run_web)
    web_thread.start()
    
    # Запуск бота в основном потоке
    asyncio.run(bot_main())