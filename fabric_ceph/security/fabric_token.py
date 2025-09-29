from typing import Tuple


class TokenException(Exception):
    """
    Token exception
    """


class FabricToken:
    """
    Represents the Fabric Token issues by Credential Manager
    """
    def __init__(self, *, decoded_token: dict, token_hash: str):
        self.decoded_token = decoded_token
        self.hash = token_hash

    @property
    def token_hash(self):
        return self.hash

    @property
    def token(self):
        return self.decoded_token

    @property
    def uuid(self) -> str:
        return self.decoded_token.get("uuid")

    @property
    def subject(self) -> str:
        """
        Get subject
        @return subject
        """
        return self.decoded_token.get("sub")

    @property
    def email(self) -> str:
        """
        Get email
        @return email
        """
        return self.decoded_token.get("email")

    @property
    def projects(self) -> list or None:
        """
        Get projects
        @return projects
        """
        return self.decoded_token.get("projects")

    @property
    def roles(self) -> list:
        return self.decoded_token.get("roles")

    def __str__(self):
        return f"Token: {self.decoded_token}"

    @property
    def first_project(self) -> Tuple[str or None, str or None, str or None]:
        if self.projects is None or len(self.projects) == 0:
            return None, None, None

        return self.projects[0].get("uuid"), self.projects[0].get("tags"), self.projects[0].get("name")
