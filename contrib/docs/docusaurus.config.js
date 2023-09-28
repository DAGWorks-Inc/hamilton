// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const organizationName = "dagworks-inc";
const projectName = "hamilton";  // fixed due to GitHub pages deployment

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Hamilton Dataflow Hub',
  tagline: 'Your place to find Hamilton dataflows',
  favicon: 'img/hamilton_logo_transparent_bkgrd.ico',

  // Set the production url of your site here
  url: `https://${organizationName}.github.io`,
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: `/`, // /${projectName}/`,

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: organizationName, // Usually your GitHub org/user name.
  projectName: projectName  , // Usually your repo name.

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          // editUrl:
          //   'https://github.com/dagworks-inc/contrib/hamilton/',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          // editUrl:
          //   'https://github.com/facebook/docusaurus/tree/main/packages/create-docusaurus/templates/shared/',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: 'G-TGB1H6EPP9',
        },
        sitemap: {
          changefreq: 'daily',
          priority: 0.5,
          // ignorePatterns: ['/tags/**'],
          filename: 'sitemap.xml',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      // Replace with your project's social card
      image: 'img/docusaurus-social-card.jpg',
      navbar: {
        title: 'Hamilton Dataflow Hub',
        logo: {
          alt: 'Hamilton',
          src: 'img/hamilton_logo.png',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'dataflowSidebar',
            position: 'left',
            label: 'Dataflows',
          },
          {to: '/blog', label: 'Changelog ', position: 'left'},
          {href: 'https://blog.dagworks.io', label: 'DAGWorks Blog', position: 'right'},
          {
            href: 'https://github.com/dagworks-inc/hamilton',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {
                label: 'Dataflows',
                to: '/docs/',
              },
            ],
          },
          {
            title: 'Community',
            items: [
              {
                label: 'Twitter',
                href: 'https://twitter.com/hamilton_os',
              },
              {
                label: 'Slack',
                href: 'https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg',
              },
              // {
              //   label: 'Twitter',
              //   href: 'https://twitter.com/docusaurus',
              // },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'Blog',
                to: '/blog',
              },
                {
                label: 'DAGWorks Blog',
                href: 'https://blog.dagworks.io',
              },
              {
                label: 'GitHub',
                href: 'https://github.com/dagworks-inc/hamilton',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} DAGWorks, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
