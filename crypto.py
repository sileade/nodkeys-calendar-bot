"""
Nodkeys E2E Encryption Module
Zero-knowledge architecture: server never sees plaintext user data.

Key derivation: user_id + app_secret → PBKDF2-SHA256 → Master Key → AES-256-GCM

The user never enters a password. The encryption key is derived from:
- Telegram user_id (unique per user)
- APP_ENCRYPTION_SECRET (env variable, stored separately from data)

This means:
- Server DB dump alone is useless (no APP_ENCRYPTION_SECRET)
- APP_ENCRYPTION_SECRET alone is useless (no user data)
- Both together + user_id = can decrypt that user's data only
"""

import os
import json
import hashlib
import hmac
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

# App-level secret — stored in env, separate from database
APP_ENCRYPTION_SECRET = os.environ.get("APP_ENCRYPTION_SECRET", "nodkeys-default-secret-change-me")

# PBKDF2 iterations (high for security)
KDF_ITERATIONS = 150_000


def _derive_master_key(user_id: int) -> bytes:
    """Derive a unique 256-bit master key for a user.
    
    Uses PBKDF2-SHA256 with:
    - password = APP_ENCRYPTION_SECRET
    - salt = user_id as bytes (unique per user)
    - iterations = 150,000
    
    Returns: 32-byte key
    """
    salt = f"nodkeys-user-{user_id}".encode('utf-8')
    password = APP_ENCRYPTION_SECRET.encode('utf-8')
    
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=KDF_ITERATIONS,
    )
    return kdf.derive(password)


def encrypt_data(user_id: int, plaintext: str) -> str:
    """Encrypt plaintext data for a specific user.
    
    Uses AES-256-GCM (authenticated encryption).
    
    Args:
        user_id: Telegram user ID
        plaintext: JSON string or any text to encrypt
    
    Returns:
        Base64-encoded string: nonce (12 bytes) + ciphertext + tag (16 bytes)
    """
    key = _derive_master_key(user_id)
    aesgcm = AESGCM(key)
    
    # Generate random 96-bit nonce
    nonce = os.urandom(12)
    
    # Encrypt
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode('utf-8'), None)
    
    # Return nonce + ciphertext as base64
    return base64.b64encode(nonce + ciphertext).decode('ascii')


def decrypt_data(user_id: int, encrypted: str) -> str:
    """Decrypt data for a specific user.
    
    Args:
        user_id: Telegram user ID
        encrypted: Base64-encoded string from encrypt_data()
    
    Returns:
        Decrypted plaintext string
    
    Raises:
        ValueError: if decryption fails (wrong key or tampered data)
    """
    key = _derive_master_key(user_id)
    aesgcm = AESGCM(key)
    
    raw = base64.b64decode(encrypted)
    nonce = raw[:12]
    ciphertext = raw[12:]
    
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode('utf-8')
    except Exception as e:
        raise ValueError(f"Decryption failed: {e}")


def encrypt_json(user_id: int, data: dict) -> str:
    """Encrypt a dictionary as JSON for a user."""
    return encrypt_data(user_id, json.dumps(data, ensure_ascii=False))


def decrypt_json(user_id: int, encrypted: str) -> dict:
    """Decrypt and parse JSON data for a user."""
    plaintext = decrypt_data(user_id, encrypted)
    return json.loads(plaintext)


def generate_api_token(user_id: int) -> str:
    """Generate a unique API token for shortcuts/external access.
    
    Token is deterministic (same user_id always gets same token)
    so it can be verified without storing it.
    """
    key = _derive_master_key(user_id)
    token_raw = hmac.new(key, f"api-token-{user_id}".encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(token_raw).decode('ascii').rstrip('=')


def verify_api_token(token: str, user_id: int) -> bool:
    """Verify that an API token belongs to a user."""
    expected = generate_api_token(user_id)
    return hmac.compare_digest(token, expected)


def get_user_fingerprint(user_id: int) -> str:
    """Get a short fingerprint for display (first 8 chars of key hash).
    Useful for user to verify their encryption is active."""
    key = _derive_master_key(user_id)
    return hashlib.sha256(key).hexdigest()[:8]
