[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Za2-KmAa)
# Parallele LogLevel-Analyse

## Aufgabenstellung 1:  Einfache LogLevel-Zählung (Grundlegend)

### Ziel  

Die Schüler sollen die Grundlagen der parallelen Verarbeitung mit `ExecutorService` verstehen und anwenden, um die Häufigkeit verschiedener LogLevel in mehreren Logdateien gleichzeitig zu zählen.

### Aufgaben

1.  **Logdateien generieren:** Verwenden Sie den bereitgestellten `LogGenerator`, um eine Anzahl von Logdateien zu erstellen (z.B. 5-10 Dateien). Experimentieren Sie mit verschiedenen Dateigrößen und Dateipräfixen, um unterschiedliche Datensätze zu erzeugen.
2.  **Sequentielle Analyse (zum Vergleich):** Schreiben Sie zuerst ein Programm, das die Logdateien *sequentiell* analysiert. Dieses Programm soll jede Logdatei einzeln öffnen, zeilenweise lesen und die Anzahl der Einträge für jedes LogLevel (TRACE, DEBUG, INFO, WARN, ERROR) zählen. Geben Sie die Ergebnisse für jede Datei und eine Gesamtzusammenfassung aus. Messen Sie die Ausführungszeit dieses sequentiellen Programms.
3.  **Parallele Analyse mit `ExecutorService`:**
    *   Entwickeln Sie eine `Callable`-Klasse namens `LogAnalyzerTask`, die eine einzelne Logdatei analysiert und eine `Map` zurückgibt, die die Zählungen der LogLevel für diese Datei enthält
    *   Erstellen Sie in der `main`-Methode einen `ExecutorService` mit einer festen Anzahl von Threads (z.B. so viele Threads wie Prozessorkerne verfügbar sind).
    *   Erstellen Sie für jede Logdatei eine Instanz von `LogAnalyzerTask` und übergeben Sie diese dem `ExecutorService` zur Ausführung (`executorService.submit(...)`).
    *   Sammeln Sie die `Future`-Objekte, die von `submit()` zurückgegeben werden, in einer Liste.
    *   Iterieren Sie über die Liste der `Future`-Objekte, warten Sie auf das Ergebnis jeder Aufgabe (`future.get()`) und aggregieren Sie die Zählungen der LogLevel aus allen Dateien zu einer Gesamtzusammenfassung. Geben Sie die Ergebnisse für jede Datei und die Gesamtzusammenfassung aus. Messen Sie die Ausführungszeit dieses parallelen Programms.
4.  **Vergleich und Analyse:** Vergleichen Sie die Ausführungszeiten der sequentiellen und parallelen Programme. Diskutieren Sie, warum die parallele Version (hoffentlich) schneller ist. Erklären Sie, wie `ExecutorService` die parallele Verarbeitung ermöglicht und welche Vorteile dies bietet.

## Aufgabenstellung 2:  Erweiterte Loganalyse mit Fehler-Fokus (Vertiefend)

### Ziel 

Die Schüler sollen ihre Kenntnisse von `ExecutorService` vertiefen und eine spezifischere Analyse durchführen, die sich auf Fehler und Warnungen in den Logdateien konzentriert.

### Aufgaben:

1.  **Logdateien generieren:**  Generieren Sie Logdateien mit dem `LogGenerator`, wobei Sie darauf achten, dass auch `WARN`- und `ERROR`-Meldungen enthalten sind (die Standardverteilung im Generator erzeugt bereits Fehler und Warnungen). Erhöhen Sie evtl. die Dateigrößen.
2.  **Parallele Fehleranalyse:**
    *   Modifizieren Sie die `LogAnalyzerTask` so, dass sie nicht nur die LogLevel zählt, sondern auch **alle Logzeilen** extrahiert, die den LogLevel `ERROR` oder `WARN` haben.  Speichern Sie diese Zeilen in einer Liste für jede Datei.
    *   Die `call()`-Methode der `LogAnalyzerTask` soll nun eine `Map` zurückgeben, die zwei Einträge enthält:
        *   Einen Eintrag für die LogLevel-Zählungen (wie in Aufgabe 1).
        *   Einen Eintrag für die Liste der extrahierten `ERROR`- und `WARN`-Logzeilen.
    *   In der `main`-Methode sammeln Sie wieder die Ergebnisse der `LogAnalyzerTask`s parallel ein.
    *   Geben Sie für jede Datei die LogLevel-Zählungen aus und zusätzlich die extrahierten `ERROR`- und `WARN`-Logzeilen (oder eine begrenzte Anzahl davon, falls es sehr viele sind).
    *   Aggregieren Sie die LogLevel-Zählungen zu einer Gesamtzusammenfassung.

3.  **Fehlertypen-Analyse:**
    *   Erweitern Sie die Analyse noch weiter, um zu versuchen, **verschiedene Arten von Fehlern** zu identifizieren.  Zum Beispiel könnten Sie nach bestimmten Schlüsselwörtern in den `ERROR`-Logzeilen suchen (z.B. "NullPointerException", "FileNotFoundException", "SQLException").
    *   Erstellen Sie eine zusätzliche Map in der `LogAnalyzerTask`, um die Häufigkeit verschiedener Fehlertypen zu zählen.
    *   Geben Sie diese Fehlertypen-Zählungen ebenfalls aus.
