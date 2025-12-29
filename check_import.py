
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
import neo_api_client
from neo_api_client import neo_api
import inspect

print(f"neo_api_client file: {neo_api_client.__file__}")
print(f"neo_api module file: {neo_api.__file__}")

import inspect
lines = inspect.getsource(neo_api.NeoAPI.session_2fa)
print("Source of session_2fa:")
print(lines)
