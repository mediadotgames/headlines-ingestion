"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ARTIFACT_CONTRACT_VERSION = void 0;
exports.csvHeader = csvHeader;
exports.toCsvRow = toCsvRow;
exports.ARTIFACT_CONTRACT_VERSION = "newsapi-ai/v2";
var HEADERS = [
    "uri",
    "url",
    "title",
    "body",
    "date",
    "time",
    "date_time",
    "date_time_published",
    "lang",
    "is_duplicate",
    "data_type",
    "sentiment",
    "event_uri",
    "relevance",
    "story_uri",
    "image",
    "source",
    "authors",
    "sim",
    "wgt",
    "categories",
    "concepts",
    "links",
    "videos",
    "shares",
    "duplicate_list",
    "extracted_dates",
    "location",
    "original_article",
    "raw_article",
];
function csvHeader() {
    return HEADERS.join(",");
}
function escapeCsvCell(value) {
    if (/[",\n\r]/.test(value)) {
        return "\"".concat(value.replace(/"/g, '""'), "\"");
    }
    return value;
}
function toCsvRow(row) {
    return HEADERS.map(function (h) { var _a; return escapeCsvCell((_a = row[h]) !== null && _a !== void 0 ? _a : ""); }).join(",");
}
