<p align="center"><img src="https://github.com/ssoper/Zebec/raw/master/gh/zebec.png" alt="Zebec Logo"></p>

[![Actions Status](https://github.com/ssoper/Zebec/workflows/tests/badge.svg)](https://github.com/ssoper/Zebec/actions)
![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Funtitled-e5pxd95kofsj.runkit.sh%2F)
[![Download](https://img.shields.io/badge/download-v1.0.0-blue)](https://github.com/ssoper/Zebec/packages/108070)
[![License](https://img.shields.io/github/license/ssoper/Zebec)](https://github.com/ssoper/Zebec/blob/master/LICENSE)

# Zebec

Static site compiler. Written in Kotlin.

## Features

* HTML files compiled from Kotlin-based DSL (KTML) 🔧
* Includes a [minifier](https://yui.github.io/yuicompressor/) for CSS and JavaScript files 🗜
* Watches for changed files that are automatically compiled or minified 👁 
* Ships with a tiny web server to make local development a cinch 💅
* [Downloadable JAR](https://github.com/ssoper/Zebec/packages) that runs as a service 📦

## Roadmap

* Tests!
    * Github Actions
    * Surface test results, code coverage via badges
* Templating
* Blogging
    * List entries
    * Entry index
    * Unsplash
    * Keywords/Tagging
* Generate site files
    * [robots.txt](https://support.google.com/webmasters/answer/6062596?hl=en&ref_topic=6061961)
    * [sitemap](https://support.google.com/webmasters/answer/183668?hl=en&ref_topic=4581190)
* Generate shields.io badges (examples: w3c validation, link backs, etc.)
* Saved state of compiled files via local database (SQL file)
