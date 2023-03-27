function parseSubRepo(file) {
    return `IFNULL(REGEXP_EXTRACT(${file}, "(tools|examples)/[^/]+/.*"), "unknown")`;
}

function parseType(file) {
    return `IFNULL(REGEXP_EXTRACT(${file}, "(tools|examples)/[^/]+/.*"), "unknown")`;
}

module.exports = {
    parseSubRepo,
    parseType
}