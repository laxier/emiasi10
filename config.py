import os
from pathlib import Path

try:
	from dotenv import load_dotenv  # python-dotenv is in requirements
	# Загружаем .env из корня проекта (на уровень выше текущей папки) и из текущего каталога
	project_root = Path(__file__).resolve().parent.parent
	load_dotenv(project_root / '.env')
	load_dotenv(Path(__file__).resolve().parent / '.env')
except Exception:
	# Библиотека может отсутствовать в ранней среде – просто игнорируем
	pass

# Telegram bot token (настраивается через переменную окружения TELEGRAM_BOT_TOKEN или .env)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

if not TELEGRAM_BOT_TOKEN:
	# Попытка fallback: файл token.txt в корне проекта (необязательно)
	token_file = Path(__file__).resolve().parent.parent / 'token.txt'
	if token_file.exists():
		try:
			TELEGRAM_BOT_TOKEN = token_file.read_text(encoding='utf-8').strip()
		except Exception:
			pass

# Base URL for the EMIAS API
EMIAS_API_BASE_URL = os.environ.get("EMIAS_API_BASE_URL", "https://emias.info/api-eip/")

def require_token():
	"""Бросает понятную ошибку, если токен отсутствует."""
	if not TELEGRAM_BOT_TOKEN:
		raise RuntimeError(
			"TELEGRAM_BOT_TOKEN не задан. Создайте .env с TELEGRAM_BOT_TOKEN=xxxx или экспортируйте переменную окружения."
		)

