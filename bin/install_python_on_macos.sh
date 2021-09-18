# First make sure you have these in .bashrc
# export PATH="/usr/local/opt/tcl-tk/bin:$PATH"
# export LDFLAGS="-L/usr/local/opt/tcl-tk/lib"
# export CPPFLAGS="-I/usr/local/opt/tcl-tk/include"
# export PKG_CONFIG_PATH="/usr/local/opt/tcl-tk/lib/pkgconfig"

#brew update
#brew install tcl-tk zlib readline xz bzip2

python_version_path=$(dirname $BASH_SOURCE)/..
python_version=$(cat $python_version_path/.python-version)

env \
  PATH="$(brew --prefix tcl-tk)/bin:$PATH" \
  CFLAGS="-I$(brew --prefix tcl-tk)/include -I$(brew --prefix zlib)/include -I$(brew --prefix openssl)/include -I$(brew --prefix bzip2)/include -I$(brew --prefix readline)/include -I$(xcrun --show-sdk-path)/usr/include" \
  LDFLAGS="-L$(brew --prefix tcl-tk)/lib -L$(brew --prefix bzip2)/lib -L$(brew --prefix zlib) -L$(brew --prefix openssl)/lib -L$(brew --prefix readline)/lib" \
  CPPFLAGS="-I$(brew --prefix tcl-tk)/include -I$(brew --prefix bzip2)/include -I$(brew --prefix zlib)/include" \
  PKG_CONFIG_PATH="$(brew --prefix tcl-tk)/lib/pkgconfig" \
  PYTHON_CONFIGURE_OPTS="--with-tcltk-includes='-I$(brew --prefix tcl-tk)/include' --with-tcltk-libs='-L$(brew --prefix tcl-tk)/lib -ltcl8.6 -ltk8.6'" \
  pyenv install $python_version
