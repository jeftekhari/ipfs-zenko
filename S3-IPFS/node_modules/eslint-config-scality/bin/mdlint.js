#!/usr/bin/env node

'use strict'; // eslint-disable-line strict

const commander = require('commander');
const markdownlint = require('markdownlint');

const files = commander.parse(process.argv).args;

// See rules at https://github.com/mivok/markdownlint/blob/master/docs/RULES.md
const config = {
    MD004: false,  // Unordered list style
    MD007: false,  // Unordered list indentation
    MD024: false,  // Multiple headers with the same content
    MD027: false,  // Multiple spaces after blockquote symbol
    MD029: { style: 'ordered' }, // Ordered list item prefix
    MD034: false,  // Bare URL used
    MD040: false,  // Fenced code blocks should have a language specified
};

const result = markdownlint.sync({ files, config });
const errors = result.toString();
if (errors) {
    process.stderr.write(`${errors}\n`);
    process.exit(1);
}
