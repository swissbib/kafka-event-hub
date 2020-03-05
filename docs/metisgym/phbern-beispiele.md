# Price

Für price muss die Information aus Kosten übernommen und in einen string umgewandelt werden.
Beispiel:

"kosten": [
                              {
                                                  "sort": 1,
                                                  "position": "vos",
                                                  "wert": 900.0,
                                                  "preiskategorie": null
                              },
                              {
                                                  "sort": 2,
                                                  "position": "sek2",
                                                  "wert": 900.0,
                                                  "preiskategorie": null
                              },
                              {
                                                  "sort": 4,
                                                  "position": "dritte",
                                                  "wert": 1500.0,
                                                  "preiskategorie": null
                              }
          ],
          
Output: Volksschule (Kt. Bern): CHF 900, SEK II (Kt. Bern): CHF 900, Dritte: CHF 1500

Dazu den Inhalt in position übersetzen und als Vorspann vor den Inhalt aus wert setzen. Zudem Interpunktion und Währung ergänzen.

Übersetzungen für die Werte in position:
vos = Volksschule (Kt. Bern)
sek2 = SEK II (Kt. Bern)
dritte = Dritte
mat = Material
gef = Gesundheits- und Fürsorgedirektion
hsk = Heimatliche Sprache und Kultur
verpfl = Verpflegung



# Dates

Für dates muss die Information aus termine übernommen und in einen string umgewandelt werden.
Beispiel:

"termine": [
                    {
                                        "start": "2020-10-16T09:00:00",
                                        "ende": "2020-10-16T17:00:00"
                    },
                    {
                                        "start": "2020-10-17T09:00:00",
                                        "ende": "2020-10-17T17:00:00"
                    },
                    {
                                        "start": "2020-11-20T09:00:00",
                                        "ende": "2020-11-20T17:00:00"
                    },
                    {
                                        "start": "2020-11-21T09:00:00",
                                        "ende": "2020-11-21T17:00:00"
                    },
                    {
                                        "start": "2020-12-11T09:00:00",
                                        "ende": "2020-12-11T17:00:00"
                    }
],

Output: 16.10.2020 von 9:00 bis 17:00 Uhr, 17.10.2020 von 9:00 bis 17:00 Uhr, 20.11.2020 von 9:00 bis 17:00 Uhr, 21.11.2020 von 9:00 bis 17:00 Uhr, 11.12.2020 von 9:00 bis 17:00 Uhr