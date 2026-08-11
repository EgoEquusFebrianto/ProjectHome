from flask import jsonify

from app.models.user_model import users

def get_users():
    return jsonify([
        user.to_dict() for user in users
    ])