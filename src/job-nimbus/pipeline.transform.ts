import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
dayjs.extend(customParseFormat);
import { round } from 'lodash';

export const transformNull = (value?: string) => {
    return value !== '' ? value && value.replace('"', '') : undefined;
};

export const transformDate = (value?: string) => {
    return value ? dayjs(value, 'M/D/YYYY').format('YYYY-MM-DDTHH:mm:ss') : undefined;
};

export const transformDateTime = (value?: string) => {
    if (value && !dayjs(value, ['M/D/YYYY H:m A', 'M/D/YYYY']).isValid()) {
        console.log(value);
    }
    return value
        ? dayjs(value, ['M/D/YYYY H:m A', 'M/D/YYYY']).format('YYYY-MM-DDTHH:mm:ss')
        : undefined;
};

export const transformFloat = (value?: string) => {
    if (value && value !== 'N/A') {
        return round(
            parseFloat(value.replace('$', '').replace(',', '').replace('(', '-').replace(')', '')),
            6,
        );
    }
    return;
};

export const transformBoolean = (value?: string) => {
    return value !== undefined && value !== null ? value : undefined;
};
