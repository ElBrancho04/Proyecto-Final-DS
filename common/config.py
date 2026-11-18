import os
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET", "super-secret-key-change-in-production")
LEASE_S = int(os.getenv("LEASE_S", "15"))
GOSSIP_S = int(os.getenv("GOSSIP_S", "1"))
RAY_NAMESPACE = "plataforma"