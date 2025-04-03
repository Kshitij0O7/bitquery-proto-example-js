const fs = require("fs");
const xlsx = require("xlsx");

const writeToExcel = (FILE_PATH, SHEET_NAME, writeQueue) => {
    if (writeQueue.length === 0) return;

    let workbook;
    let worksheet;
    let jsonData = [];

    // Check if file exists
    if (fs.existsSync(FILE_PATH)) {
        workbook = xlsx.readFile(FILE_PATH);
        worksheet = workbook.Sheets[SHEET_NAME];
        if (worksheet) {
            jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
        }
    } else {
        workbook = xlsx.utils.book_new();
        worksheet = xlsx.utils.aoa_to_sheet([['Hash', 'Time', 'Timestamp']]); // Headers
        xlsx.utils.book_append_sheet(workbook, worksheet, SHEET_NAME);
    }

    if (!worksheet) {
        worksheet = xlsx.utils.aoa_to_sheet([['Hash', 'Time', 'Timestamp']]); // Headers
        xlsx.utils.book_append_sheet(workbook, worksheet, SHEET_NAME);
    }

    // Convert existing data to a Set for quick lookup of existing (partition, offset) pairs
    const existingKeys = new Set(jsonData.slice(1).map(row => row[0]));

    // Append only unique messages
    const newEntries = [];
    while (writeQueue.length > 0) {
        const [hash, time, timestamp] = writeQueue.shift();
        const key = hash;

        if (!existingKeys.has(key)) { // Avoid duplicate entries
            newEntries.push([hash, time, timestamp]);
            existingKeys.add(key);
        }
    }

    if (newEntries.length > 0) {
        jsonData.push(...newEntries);
        const newWorksheet = xlsx.utils.aoa_to_sheet(jsonData);
        workbook.Sheets[SHEET_NAME] = newWorksheet;
        xlsx.writeFile(workbook, FILE_PATH);
    }
};

module.exports = {writeToExcel};