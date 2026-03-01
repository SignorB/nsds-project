CONTIKI_PROJECT = smart-grid-mote
all: $(CONTIKI_PROJECT)

# Project configuration
PROJECT_CONF_PATH = project-conf.h

# Define the location of the Contiki-NG OS relative to this directory
CONTIKI = ../contiki-ng

# Include the MQTT library
MODULES += os/net/app-layer/mqtt

# Standard Contiki include
include $(CONTIKI)/Makefile.include
