DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE Title de.l3s.archivepig.enrich.HtmlText('title');
DEFINE ProperNouns de.l3s.archivepig.enrich.ProperNouns;

DEFINE Host de.l3s.archivepig.get.Host;
DEFINE Get de.l3s.archivepig.get.Get;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';

cdxdata = LIMIT cdxdata 5;

titles = FOREACH cdxdata GENERATE Title(record);

wellness = FILTER titles BY (chararray)Get(record, '.title.text') MATCHES '.*Wellness.*';

nouns = FOREACH wellness GENERATE ProperNouns(record);

STORE nouns INTO 'results/11' USING ExtractionStorage;

