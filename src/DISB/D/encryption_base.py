from cryptography.fernet import Fernet, InvalidToken  # Moved InvalidToken here
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend
import base64  # For encoding/decoding Fernet keys and hashes to store as strings
import hashlib  # For hashing user-provided seeds
import string  # For nickname generation
import random
class encrybase:
    def __init__(self, salt,info, log):
        self.log = log
        self._HKDF_SALT = salt
        self._HKDF_INFO = info
    def _derive_key_from_seed(self, seed: str) -> bytes:
        """Derives a Fernet key from a user-provided seed using HKDF."""
        if not seed:
            raise ValueError("Encryption seed cannot be empty.")
        kdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self._HKDF_SALT,
            info=self._HKDF_INFO,
            backend=default_backend()
        )
        # Use a strong hash of the seed as the input key material
        input_key_material = hashlib.sha256(seed.encode('utf-8')).digest()
        key = base64.urlsafe_b64encode(kdf.derive(input_key_material))
        self.log.debug(f"Derived Fernet key from seed.")
        return key


    def _encrypt_data(self, data: bytes, key: bytes) -> bytes:
        """Encrypts data using Fernet."""
        f = Fernet(key)
        return f.encrypt(data)


    def _decrypt_data(self, encrypted_data: bytes, key: bytes) -> bytes:
        """Decrypts data using Fernet."""
        f = Fernet(key)
        return f.decrypt(encrypted_data)


    def _generate_random_nickname(self, length=8) -> str:
        """Generates a random alphanumeric nickname."""
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for i in range(length))