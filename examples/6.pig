DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE Html de.l3s.archivepig.enrich.Html('body');

DEFINE Host de.l3s.archivepig.get.Host;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';

cdxdata = LIMIT cdxdata 5;

html = FOREACH cdxdata GENERATE Html(record);

STORE html INTO 'results/6' USING ExtractionStorage;

