.PHONY: create_requirements
create_requirements:
	poetry export --without-hashes --format=requirements.txt > requirements.txt

.PHONY: lint
lint:
	pre-commit run --all-files

.PHONY: concatenate_and_analyze
concatenate_and_analyze:
	python3 twitter_search/concat_csv_files.py
	python3 twitter_search/run_analysis.py
