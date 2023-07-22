# How to set up environment

- Install [pyenv](https://github.com/pyenv/pyenv#installation).

- Install Python.
  *  If on MacOS, use this (handles some requirements and compiler paths):
      ```
      bin/install_python_on_macos.sh
      ```
  * If not on MacOS install Python:
    ```
    pyenv install $(cat .python-version)
    ```

- Create a virtualenv
  ```
  python -m venv .venv
  ```

- Install [poetry](https://python-poetry.org/docs/#installation) (inside the virtual env).

- Install requirements:
  ```
  brew install geos
  poetry install
  ```
