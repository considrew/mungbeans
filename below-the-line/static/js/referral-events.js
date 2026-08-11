/* Referral click tracking for GA4.
 *
 * One delegated listener on the document. Any anchor carrying rel="sponsored"
 * fires a `referral_click` event, so adding a tool to data/tools.yml needs no
 * change here — the link inherits rel="sponsored" from the partials and starts
 * reporting on its own.
 *
 * Deliberately derives everything from the DOM rather than requiring data
 * attributes, because the standalone deep-dive articles in static/ are written
 * by hand and would drift out of sync with the templates.
 */
(function () {
  'use strict';

  if (typeof document === 'undefined') return;

  // hostname -> short name used as the `tool` dimension in GA4.
  var TOOLS = {
    'godelterminal.com': 'godel',
    'app.godelterminal.com': 'godel',
    'simplywall.st': 'simplywallst',
    'tipranks.com': 'tipranks',
    'www.tipranks.com': 'tipranks'
  };

  function toolFor(hostname) {
    if (TOOLS[hostname]) return TOOLS[hostname];
    // Fall back to the registrable-looking part so a new referral still reports
    // something legible instead of being dropped.
    var parts = hostname.replace(/^www\./, '').split('.');
    return parts.length > 1 ? parts[parts.length - 2] : hostname;
  }

  // Where on the site the click happened. Kept coarse on purpose: these become
  // GA4 custom dimension values, and high-cardinality values are useless in
  // reports and count against the property's dimension limits.
  function placement(path) {
    if (path === '/' || path === '/index.html') return 'home';
    if (path.indexOf('/tools') === 0) return 'tools-page';
    if (path.indexOf('/deep-dives/') === 0 && path !== '/deep-dives/') return 'deep-dive-article';
    if (path.indexOf('/deep-dives') === 0) return 'deep-dives-index';
    if (path.indexOf('/blog/') === 0 && path !== '/blog/') return 'weekly-report';
    if (path.indexOf('/microcaps') === 0) return 'microcaps';
    if (path.indexOf('/book') === 0) return 'book';
    if (path.indexOf('/disclaimer') === 0) return 'disclaimer';
    if (path === '/stocks/' || path === '/stocks') return 'all-stocks';
    if (path.indexOf('/stocks/') === 0) return 'stock-page';
    return 'other';
  }

  // The referral box carries a CTA button, an inline strip and a bare text
  // link. Knowing which one converts is the whole point of measuring this.
  function linkStyle(el) {
    if (el.classList.contains('tool-rec-btn')) return 'button';
    if (el.closest('.tool-inline')) return 'inline-strip';
    if (el.closest('.tool-rec-shot')) return 'screenshot';
    if (el.closest('.tool-entry')) return 'tools-page-entry';
    if (el.closest('.tool-rec')) return 'box-link';
    return 'text';
  }

  document.addEventListener('click', function (event) {
    var link = event.target.closest && event.target.closest('a[rel~="sponsored"]');
    if (!link || !link.href) return;

    var url;
    try {
      url = new URL(link.href);
    } catch (e) {
      return;
    }

    if (typeof window.gtag !== 'function') return;

    var path = window.location.pathname;

    window.gtag('event', 'referral_click', {
      tool: toolFor(url.hostname),
      placement: placement(path),
      link_style: linkStyle(link),
      link_url: link.href,
      page_path: path,
      // GA4 groups these under the standard outbound-link reporting too.
      link_domain: url.hostname,
      outbound: true
    });
  }, true);
})();
