document.addEventListener('DOMContentLoaded', function() {
    // --- ОБНОВЛЕННАЯ ЛОГИКА ПЕРЕКЛЮЧЕНИЯ ВАЛЮТ ---

    // 1. Находим оба возможных элемента управления
    const currencyToggle = document.getElementById('currencyToggle'); // Старый переключатель
    const currencySelector = document.getElementById('currency'); // Новый выпадающий список
    // --- ИЗМЕНЕНИЕ: Убираем || 12650 ---
    const usdRate = parseFloat(document.body.dataset.usdRate);

    // 2. Функция для форматирования чисел
    function formatNumber(num) {
        return new Intl.NumberFormat('ru-RU').format(Math.round(num));
    }

    // 3. Единая функция для обновления валюты в интерфейсе
    function updateCurrencyDisplay() {
        let isUSD = false;
        let currency = 'UZS';

        // Определяем, какой элемент управления активен и получаем его значение
        if (currencySelector) {
            isUSD = currencySelector.value === 'USD';
            currency = currencySelector.value;
        } else if (currencyToggle) {
            isUSD = currencyToggle.checked;
            currency = isUSD ? 'USD' : 'UZS';
        }

        // Обновляем все элементы с классом .currency-value
        document.querySelectorAll('.currency-value').forEach(el => {
            const uzsValue = parseFloat(el.dataset.uzsValue);
            if (isNaN(uzsValue)) return;

            if (isUSD && usdRate) { // Добавляем проверку, что курс есть
                el.textContent = '$ ' + formatNumber(uzsValue / usdRate);
            } else {
                el.textContent = formatNumber(uzsValue);
            }
        });

        // Обновляем текстовые метки (например, в заголовках таблиц)
        document.querySelectorAll('#currencyLabel, .table-currency-label, .chart-currency-label').forEach(label => {
            if(label) label.textContent = currency;
        });

        // Обновляем ссылку для экспорта, добавляя в нее выбранную валюту
        const exportLink = document.getElementById('export-link');
        if (exportLink) {
            const baseUrl = exportLink.dataset.baseUrl;
            exportLink.href = `${baseUrl}?currency=${currency}`;
        }

        // Сохраняем выбор пользователя
        localStorage.setItem('reportCurrency', currency);
    }

    // 4. Устанавливаем начальное состояние при загрузке страницы
    const savedCurrency = localStorage.getItem('reportCurrency');
    if (savedCurrency) {
        if (currencySelector) {
            currencySelector.value = savedCurrency;
        }
        if (currencyToggle) {
            currencyToggle.checked = savedCurrency === 'USD';
        }
    }
    updateCurrencyDisplay(); // Вызываем для первоначальной отрисовки

    // 5. Привязываем обработчики событий к элементам, если они существуют
    if (currencySelector) {
        currencySelector.addEventListener('change', updateCurrencyDisplay);
    }
    if (currencyToggle) {
        currencyToggle.addEventListener('change', updateCurrencyDisplay);
    }


    // --- Логика фильтрации (без изменений) ---
    const searchInput = document.querySelector('input[type="text"]#searchInput');
    const reportRows = document.querySelectorAll('.report-row');
    const hideZeroPlanToggle = document.getElementById('hideZeroPlanToggle');

    function applyFilters() {
        const searchTerm = searchInput ? searchInput.value.toLowerCase() : '';
        const hideZeroPlan = hideZeroPlanToggle ? hideZeroPlanToggle.checked : false;

        reportRows.forEach(row => {
            const projectNameElement = row.querySelector('.report-row-title a');
            if (!projectNameElement) return;

            const projectName = projectNameElement.textContent.toLowerCase();
            const planUnits = parseInt(row.dataset.planUnits, 10) || 0;

            const searchMatch = projectName.includes(searchTerm);
            const planMatch = !hideZeroPlan || planUnits > 0;

            if (searchMatch && planMatch) {
                row.style.display = 'block';
            } else {
                row.style.display = 'none';
            }
        });
    }

    if (searchInput) {
        searchInput.addEventListener('input', applyFilters);
    }
    if (hideZeroPlanToggle) {
        hideZeroPlanToggle.addEventListener('change', applyFilters);
    }

    applyFilters();
});