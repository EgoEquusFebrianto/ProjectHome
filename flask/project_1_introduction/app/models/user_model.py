class User:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
        }

users = [
    User(1, "Febrianto", "febrianto@example.com"),
    User(2, "Budi", "budi@example.com"),
    User(3, "Andi", "andi@example.com")
]