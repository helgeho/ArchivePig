DEFINE ArchiveLoader de.l3s.archivepig.ArchiveLoader('/data/ia/w/de');
DEFINE ExtractionStorage de.l3s.archivepig.ExtractionStorage;

DEFINE Response de.l3s.archivepig.enrich.Response;
DEFINE StringContent de.l3s.archivepig.enrich.StringContent;
DEFINE Html de.l3s.archivepig.enrich.Html('body');
DEFINE HtmlText de.l3s.archivepig.enrich.HtmlText('body');
DEFINE ProperNouns de.l3s.archivepig.enrich.ProperNouns;
DEFINE TitleNouns de.l3s.archivepig.enrich.Pipe('de.l3s.archivepig.enrich.HtmlText(title)', 'de.l3s.archivepig.enrich.ProperNouns');

DEFINE Get de.l3s.archivepig.get.Get;
DEFINE Host de.l3s.archivepig.get.Host;

cdxdata = LOAD 'data/ia/derivatives/de/cdx/*/*.cdx' USING ArchiveLoader;

cdxdata = FILTER cdxdata BY Host(record) == 'entspannungs-shop.de';

cdxdata = LIMIT cdxdata 5;

html = FOREACH cdxdata GENERATE Html(record);

STORE html INTO 'results/12' USING ExtractionStorage;


