from app.services.user_service import get_users

def index():
    return get_users()