import pickle
from cryptography.fernet import Fernet
import os
import sys


class Encrypter:
	def __init__(self, key_path):
		self.key_path = key_path
	
	def generate_key(self):
		key = Fernet.generate_key()
		with open(self.key_path, 'wb') as f:
			f.write(key)
		print(f"Key saved to {self.key_path}")
	
	def _load_fernet(self):
		if not os.path.exists(self.key_path):
			raise FileNotFoundError("Encryption key not found. Generate it first.")
		with open(self.key_path, 'rb') as f:
			key = f.read()
		return Fernet(key)
	
	def get_encrypted_data(self, data):
		fernet = self._load_fernet()
		return fernet.encrypt(data.encode())
	
	def get_decrypt_data(self, data):
		fernet = self._load_fernet()
		if isinstance(data, str):
			data = data.encode()
		return fernet.decrypt(data).decode()


if __name__ == "__main__":
	original_value = sys.argv[1]
	enc = Encrypter("/analyticsShare/yushan/04.daily_use/5.all_etl/v1/config/pickle/pass1.pkl")
	encrypted = enc.get_encrypted_data(original_value)
	print("Encrypted:", encrypted)
