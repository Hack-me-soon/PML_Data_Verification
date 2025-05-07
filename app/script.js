document.getElementById("uploadForm").addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = new FormData();
    formData.append("master_data", document.getElementById("masterFile").files[0]);
    formData.append("increment_data", document.getElementById("incrementFile").files[0]);
    formData.append("current_month_data", document.getElementById("currentFile").files[0]);
    formData.append("previous_month_data", document.getElementById("previousFile").files[0]);

    try {
        const response = await fetch("/upload-files/", {
            method: "POST",
            body: formData
        });

        if (!response.ok) {
            throw new Error(`Upload failed with status: ${response.status}`);
        }

        const result = await response.json();
        console.log("Upload success", result);

        const sheetForm = document.getElementById("sheetForm");
        sheetForm.innerHTML = ""; // Clear any previous dropdowns

        // For each file category, generate a dropdown
        for (const [fileLabel, sheets] of Object.entries(result)) {
            const label = document.createElement("label");
            label.textContent = `Select sheet for ${fileLabel}:`;
            const select = document.createElement("select");
            select.name = fileLabel;
            select.required = true;

            sheets.forEach(sheet => {
                const option = document.createElement("option");
                option.value = sheet;
                option.textContent = sheet;
                select.appendChild(option);
            });

            sheetForm.appendChild(label);
            sheetForm.appendChild(document.createElement("br"));
            sheetForm.appendChild(select);
            sheetForm.appendChild(document.createElement("br"));
        }

        // Add submit button again (it was cleared with innerHTML)
        const processBtn = document.createElement("button");
        processBtn.type = "submit";
        processBtn.textContent = "Process Selected Sheets";
        sheetForm.appendChild(processBtn);

        document.getElementById("sheetSelectionSection").style.display = "block";

    } catch (error) {
        console.error("Error uploading files:", error);
    }
});

document.getElementById("sheetForm").addEventListener("submit", async (e) => {
    e.preventDefault();

    const current_sheet = document.querySelector("select[name='current_file']").value;
    const previous_sheet = document.querySelector("select[name='previous_file']").value;
    const increment_sheet = document.querySelector("select[name='increment_file']").value;
    const master_sheet = document.querySelector("select[name='master_file']").value;

    const payload = {
        current_sheet,
        previous_sheet,
        increment_sheet,
        master_sheet
    };

    try {
        const response = await fetch("/process-selected-sheets/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`Sheet selection failed: ${response.status}`);
        }

        const result = await response.json();
        console.log("Sheet selection success", result);

        // ✅ Show the "Run Process Operation" button after success
        document.getElementById("processOperationButton").style.display = "block";

        alert("Sheets selected successfully.");

    } catch (error) {
        console.error("Error submitting sheet selection:", error);
    }
});  

function showSheetSelectionSection(data) {
    const form = document.getElementById("sheetForm");
    form.innerHTML = ""; // Clear previous dropdowns

    const files = ["master_file", "increment_file", "current_file", "previous_file"];
    files.forEach(file => {
        const label = document.createElement("label");
        label.textContent = `Select Sheet for ${file.replace("_file", "").toUpperCase()}:`;
        form.appendChild(label);

        const select = document.createElement("select");
        select.id = `${file}_sheet`;
        data[file].forEach(sheet => {
            const option = document.createElement("option");
            option.value = sheet;
            option.textContent = sheet;
            select.appendChild(option);
        });
        form.appendChild(select);
        form.appendChild(document.createElement("br"));
    });

    const button = document.createElement("button");
    button.textContent = "Process Selected Sheets";
    button.addEventListener("click", function(e) {
        e.preventDefault();
        processSelectedSheets(data);
    });
    form.appendChild(button);

    document.getElementById("sheetSelectionSection").style.display = "block";
}

// Handle process-operation call
document.getElementById("processOperationButton").addEventListener("click", async () => {
    const spinner = document.getElementById("loadingSpinner");
    spinner.style.display = "inline-block"; // Show spinner

    try {
        const response = await fetch("/process-operation/", {
            method: "POST"
        });

        if (!response.ok) {
            throw new Error(`Process operation failed: ${response.status}`);
        }

        const blob = await response.blob();
        const downloadUrl = URL.createObjectURL(blob);
        const downloadLink = document.getElementById("downloadLink");

        downloadLink.href = downloadUrl;
        downloadLink.download = "final_output.xlsx";
        document.getElementById("resultSection").style.display = "block";
        alert("Operation completed. Download the output file.");


    } catch (error) {
        console.error("Error during process operation:", error);
        alert("Process operation failed.");
    } finally {
        spinner.style.display = "none"; // Hide spinner regardless of outcome
    }
});



async function processSelectedSheets(data) {
    const selectedSheets = {
        masterSheet: document.getElementById("master_file_sheet").value,
        incrementSheet: document.getElementById("increment_file_sheet").value,
        currentSheet: document.getElementById("current_file_sheet").value,
        previousSheet: document.getElementById("previous_file_sheet").value
    };

    try {
        const response = await fetch("/process-selected-sheets/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(selectedSheets)
        });
        const result = await response.json();
        console.log(result);

        if (result.message === "Sheet selection success" || result.message === "Sheets selected and saved.") {
            // ✅ Show the "Run Process Operation" button
            document.getElementById("processOperationButton").style.display = "block";
            alert("Sheets selected successfully.");
        }
    } catch (error) {
        console.error("Error processing selected sheets:", error);
    }
}

async function processOperation() {
    try {
        const response = await fetch("/process-operation/", {
            method: "POST"
        });
        const result = await response.blob();

        const link = document.getElementById("downloadLink");
        const url = window.URL.createObjectURL(result);
        link.href = url;
        link.style.display = "block";
    } catch (error) {
        console.error("Error processing operation:", error);
    }
}
