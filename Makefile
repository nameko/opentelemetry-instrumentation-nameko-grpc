static:
	pre-commit run --all-files

test:
	py.test tests -v

coverage:
	coverage run -m nameko test -v
	coverage report