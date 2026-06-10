poetry config repositories.test-pypi should
@echo off
set /p "token=TestPyPi Token: "
poetry config pypi-token.test-pypi %token%
cd ../
poetry publish --build -r test-pypi
PAUSE