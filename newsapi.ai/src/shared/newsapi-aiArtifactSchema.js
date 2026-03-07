"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.csvHeader = csvHeader;
exports.toCsvRow = toCsvRow;
const HEADERS = [
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
];
function csvHeader() {
    return HEADERS.join(",");
}
function escapeCsvCell(value) {
    if (/[",\n\r]/.test(value)) {
        return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
}
function toCsvRow(row) {
    return HEADERS.map((h) => escapeCsvCell(row[h] ?? "")).join(",");
}
