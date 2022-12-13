curl -sSL https://install.python-poetry.org | python - --version 1.2.0a2
export PATH=$PATH:~%USERPROFILE%\.poetry\bin
export PATH=$PATH:~%APPDATA%\Python\Scripts
# poentry version gibt die aktuell Version aus und prüft ob alles geklappt hat
poetry version
# poetry config initialisiert die config Datei unter %APPDATA%\pypoetry\config.toml
poetry config
# gibt die aktuelle config in der Konsole aus
poetry config --list
# das interne repository listen