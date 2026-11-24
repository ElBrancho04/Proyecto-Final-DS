.PHONY: up down test demo chaos

build:
	@echo "Entorno listo."

up:
	@ray status > /dev/null 2>&1 || ray start --head
	python3 -m runtime.bootstrap

down:
	python3 -c "from runtime.bootstrap import stop_platform; stop_platform()"
	ray stop

test:
	pytest

demo:
	@echo "Demo pendiente de fases posteriores."

chaos:
	@echo "Chaos tests pendientes de fases posteriores."
