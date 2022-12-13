@echo off
set /p "token=PyPi Token: "
poetry config pypi-token.pypi %token%
cd ../
poetry publish --build