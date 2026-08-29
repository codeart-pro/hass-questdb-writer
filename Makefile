.PHONY: up down logs status validate

up:
	podman compose up -d

down:
	podman compose down

logs:
	podman compose logs -f --tail=200

status:
	podman compose ps

validate:
	podman compose config
