from flask import Flask

# APPLICATION FACTORY PATTERN

def create_app():
    app = Flask(__name__)

    from app.routes.user_route import UserRoute

    app.register_blueprint(UserRoute)

    return app