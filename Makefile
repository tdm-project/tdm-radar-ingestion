
all: images

images: radar-ingestion

radar-ingestion: docker/Dockerfile.radar_ingestion
	docker build -f docker/Dockerfile.radar_ingestion -t tdmproject/radar-ingestion .

