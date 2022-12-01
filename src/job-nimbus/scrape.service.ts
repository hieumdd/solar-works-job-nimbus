import chromium from 'chrome-aws-lambda';
import playwright from 'playwright';
import csv from 'csvtojson';

export const scrapeService = async (url: string): Promise<Record<string, any>[] | undefined> => {
    const browser = await playwright.chromium.launch({
        args: chromium.args,
        executablePath: (await chromium.executablePath) || undefined,
        headless: true,
    });

    const page = await browser.newPage();

    await page.goto('https://app.jobnimbus.com/login.aspx');
    await page.waitForSelector('#web1');
    const loginFrame = page.frameLocator('#web1');
    await loginFrame.locator('input#txtUserName').fill(process.env.JOB_NIMBUS_USERNAME || '');
    await loginFrame.locator('input#txtPassword').fill(process.env.JOB_NIMBUS_PASSWORD || '');
    await loginFrame.locator('#btnLogin').click();

    await page.waitForNavigation();

    await page.goto(url);
    await page.waitForSelector('#web1');
    const reportFrame = page.frameLocator('#web1');

    const [download] = await Promise.all([
        page.waitForEvent('download', { timeout: 60_000 }),
        reportFrame
            .locator('li[data-function="export_to_csv"] > span')
            .evaluate((el) => el.click()),
    ]);

    const data = await download.createReadStream().then((stream) => {
        if (!stream) return;

        return csv()
            .fromStream(stream)
            .then((values) => values);
    });

    await browser.close();

    return data;
};
