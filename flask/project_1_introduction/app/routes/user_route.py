from flask import Blueprint

from app.controllers.user_controller import index

UserRoute = Blueprint(
    "user",
    __name__,
    url_prefix="/api/users"
)

@UserRoute.get("")
def get_all_users():
    return index()