DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE TitleNouns de.l3s.archivepig.enrich.Pipe('de.l3s.archivepig.enrich.HtmlText(title)', 'de.l3s.archivepig.enrich.ProperNouns');

DEFINE Host de.l3s.archivepig.get.Host;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';

cdxdata = LIMIT cdxdata 5;

nouns = FOREACH cdxdata GENERATE TitleNouns(record);

STORE nouns INTO 'results/9' USING ExtractionStorage;
