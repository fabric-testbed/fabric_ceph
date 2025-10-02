from typing import Tuple, Optional, List, Dict, Any


class TokenException(Exception):
    """
    Custom exception for token errors
    """


class FabricToken:
    """
    Represents a Fabric Token issued by the Credential Manager
    """

    def __init__(self, *, decoded_token: dict, token_hash: str):
        self.decoded_token: Dict[str, Any] = decoded_token
        self._hash: str = token_hash

    @property
    def token_hash(self) -> str:
        """Return the hash of the token."""
        return self._hash

    @property
    def token(self) -> Dict[str, Any]:
        """Return the full decoded token."""
        return self.decoded_token

    @property
    def uuid(self) -> Optional[str]:
        """Return the token UUID."""
        return self.decoded_token.get("uuid")

    @property
    def subject(self) -> Optional[str]:
        """Return the subject (`sub`)."""
        return self.decoded_token.get("sub")

    @property
    def email(self) -> Optional[str]:
        """Return the email address."""
        return self.decoded_token.get("email")

    @property
    def projects(self) -> Optional[List[Dict[str, Any]]]:
        """Return the list of projects (if any)."""
        return self.decoded_token.get("projects")

    @property
    def roles(self) -> List[Dict[str, str]]:
        """Return the roles as a list of dicts."""
        return self.decoded_token.get("roles", [])

    @property
    def role_names(self) -> List[str]:
        """Return only the role names as a flat list."""
        return [r.get("name") for r in self.roles if "name" in r]

    @property
    def first_project(self) -> Tuple[Optional[str], Optional[List[str]], Optional[str]]:
        """
        Return (uuid, tags, name) of the first project if present.
        """
        if not self.projects:
            return None, None, None
        p = self.projects[0]
        return p.get("uuid"), p.get("tags"), p.get("name")

    def get_project_by_uuid(self, uuid: str) -> Optional[Dict[str, Any]]:
        """Return project dict by UUID if found."""
        if not self.projects:
            return None
        return next((p for p in self.projects if p.get("uuid") == uuid), None)

    def get_project_tags(self, uuid: str) -> List[str]:
        """Return list of tags for a given project UUID, or empty list."""
        project = self.get_project_by_uuid(uuid)
        return project.get("tags", []) if project else []

    def get_project_memberships(self, uuid: str) -> Dict[str, bool]:
        """Return memberships dict for a given project UUID, or empty dict."""
        project = self.get_project_by_uuid(uuid)
        return project.get("memberships", {}) if project else {}

    def __str__(self) -> str:
        return f"FabricToken(uuid={self.uuid}, subject={self.subject}, email={self.email})"
