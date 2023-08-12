import stableStringify from 'json-stable-stringify';
import lodash from 'lodash';
import hasha from 'hasha';
export const fingerprint = (ad) => {
  const props = lodash.pick(ad, ['id', 'price']);
  const fp = hasha(stableStringify(props));
  return fp;
};
