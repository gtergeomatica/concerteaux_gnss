#!/bin/bash
# cerco e pulisco file più vecchi di 5 giorni
find ./goGPSproject/RINEX -mtime +5 -exec rm {} \;

