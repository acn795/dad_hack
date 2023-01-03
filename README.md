# Pipeline
- Daten aus Internetz sammeln und in Spark Dataframes umwandeln
- Dataframes in Kafka ablegen
- Dataframes an anderer Stelle aus Kafka gebündelt auslesen und mit Spark verarbeiten.
- Daten visualisieren

# Dataframes
## Air pollution
| Date (Key) | location | pm_2.5 | pm_10 | o3 | no2 | so2 | co |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 2022-11-01 00:00:08+01 | blabla | 65 | 20 | 11 | 0 | 2 | 0 |

## Spritpreis
| Date (Key) | station_uuid | diesel | e5 | e10 | dieselchange | e5change | e10change
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
| 2022-11-01 00:00:08+01 | 4a6773b3-2c38-453c-812b-665be4ff6c3b | 2.099 | 1.979 | 0.000 | 0 | 1 | 0 | 

## Radzählstellen
| Date (Key) | location_id | count |
| ------ | ------ | ------ |
| 27-10-2022 | DE.HH.UP_ZAEHLSTELLEN_DATEN_4 | 4588 |

# Quellen
## Air pollution
- https://aqicn.org/city/germany/hamburg/hafen/kl.-grasbrook/
- https://api.hamburg.de/datasets/v1/luftmessnetz
## Spritpreis
- https://creativecommons.tankerkoenig.de/
    API Key: 36d397b8-02e3-a4f8-597e-ff8d58fe59a4
## Radzählstellen
- https://api.hamburg.de/datasets/v1/harazaen
- https://metaver.de/trefferanzeige?docuuid=9072E37F-8505-41F0-9332-B80C02C7E802
## OGC-Api für alles mögliche
- https://api.hamburg.de/datasets/v1
