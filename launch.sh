python setup.py install
python setup.py nosetests
sphinx-apidoc world_bank -o doc -f -e
python setup.py build_sphinx
scrapyd-deploy