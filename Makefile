.PHONY: create_requirements
create_requirements:
	poetry export --without-hashes --format=requirements.txt > requirements.txt

.PHONY: lint
lint:
	pre-commit run --all-files
