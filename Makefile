clean: # Remove workspace files
	@find . -name "__pycache__" -exec rm -rf {} +
	@find . -name ".DS_Store" -exec rm -rf {} +
	@rm -rf ./.mypy_cache
	@rm -rf ./.pytest_cache
	@rm -rf ./htmlcov
	@rm -rf ./build
	@rm -rf ./synphage.egg-info
	@rm -rf .coverage
	@rm -rf .scannerwork
	@rm -rf ./dist
	@rm -rf test/fixtures/assets_testing_folder/ncbi_download/fetch
	@rm -rf test/fixtures/assets_testing_folder/sequence_quality/genbank
	@rm -rf test/fixtures/assets_testing_folder/sequence_quality_with_history/genbank
	@rm -rf test/fixtures/assets_testing_folder/blasting/gene_identity
	@rm -rf test/fixtures/assets_testing_folder/blasting_with_history/gene_identity
	@rm -rf test/fixtures/assets_testing_folder/synteny/synteny
	@rm -rf test/fixtures/assets_testing_folder/synteny_var/synteny
	@rm -rf test/fixtures/assets_testing_folder/transform/fs
	@rm -rf test/fixtures/assets_testing_folder/transform/tables
	@rm -rf test/fixtures/assets_testing_folder/transform_2/tables
	@rm -f test/fixtures/assets_testing_folder/transform_3/tables/uniqueness.parquet
	@python -c "print('Cleaning: 👌')"

black: # Format code
	@black synphage
	@black test

flake: # Lint code
	@flake8 --ignore=E501,W503,E731,E722 --max-complexity=30 synphage
	@python -c "print('Linting: 👌')"

radon:
	@radon cc synphage-a -nc
	@python -c "print('Cyclomatic complexity: 👌')"
	
cov: # Run test and coverage
	coverage run -m pytest test/unit
	coverage xml -o temp/coverage.xml

cloc: # Counts lines of code
	cloc synphage

cloc_file: # Count the lines of code per file
	@cloc --exclude-ext=json --by-file synphage | grep synphage | awk '{print $$1" "$$4}' | termgraph

print: # Prints make targets
	@grep --color '^[^#[:space:]].*:' Makefile

report: # Launches the coverage report
	@coverage html
	@python -m http.server --directory htmlcov

type: # Verify static types
	@mypy --install-types --non-interactive synphage
	@python -c "print('Types: 👌')"

pack: # Package wheel
	@python -m build

twine: # Upload to python index
	@twine upload dist/*

# build: # Build wheel
# 	@python setup.py bdist_wheel --universal

# uninstall: # Remove wheel 
# 	@pip uninstall -y synphage

# install: # Install build wheel
# 	@pip install --find-links=dist synphage

# refresh: clean build uninstall install

all: black flake type cov 