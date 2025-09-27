import asyncio
import threading
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