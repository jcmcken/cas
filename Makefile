.PHONY: test

test:
	env -i nosetests cas/tests --with-coverage --cover-package cas
