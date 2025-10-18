import os
USE_NEO4J = os.getenv("USE_NEO4J", "true").lower() == "true"
