/*
 * This file is part of the libsigrok project.
 *
 * Copyright (C) 2021 Ralf <jr-osst@gmx.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <config.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <libsigrok/libsigrok.h>
#include "libsigrok-internal.h"

#define LOG_PREFIX "output/srdir"
#define CHUNK_SIZE (4 * 1024 * 1024)

struct out_context {
	gboolean dir_created;
	uint64_t samplerate;
	char *filename;
	size_t first_analog_index;
	size_t analog_ch_count;
	gint *analog_index_map;
	struct logic_buff {
		size_t unit_size;
		size_t alloc_size;
		uint8_t *samples;
		size_t fill_size;
		uint32_t next_chunk_num;
	} logic_buff;
	struct analog_buff {
		size_t alloc_size;
		float *samples;
		size_t fill_size;
		uint32_t next_chunk_num;
	} *analog_buff;
};

static int init(struct sr_output *o, GHashTable *options)
{
	struct out_context *outc;

	(void)options;

	if (!o->filename || o->filename[0] == '\0') {
		sr_info("srdir output module requires a file name, cannot save.");
		return SR_ERR_ARG;
	}

	outc = g_malloc0(sizeof(*outc));
	outc->filename = g_strdup(o->filename);
	o->priv = outc;

	return SR_OK;
}

static int dir_create(const struct sr_output *o)
{
	struct out_context *outc;
	struct sr_channel *ch;
	size_t ch_nr;
	size_t alloc_size;
	GVariant *gvar;
	GKeyFile *meta;
	GSList *l;
	const char *devgroup;
	char *s, *metabuf;
	gsize metalen;
	guint logic_channels, enabled_logic_channels;
	guint enabled_analog_channels;
	guint index;
	FILE *f;

	outc = o->priv;

	sr_dbg("%s", __FUNCTION__);

	if (outc->samplerate == 0 && sr_config_get(o->sdi->driver, o->sdi, NULL,
					SR_CONF_SAMPLERATE, &gvar) == SR_OK) {
		outc->samplerate = g_variant_get_uint64(gvar);
		g_variant_unref(gvar);
	}

	if (g_mkdir(outc->filename, 0777)) {
		sr_err("Could not create directory: %s", strerror(errno));
		return SR_ERR;
	}
	if (g_chdir(outc->filename)) {
		sr_err("Could not change directory: %s", strerror(errno));
		return SR_ERR;
	}

	/* "version" */
	f = g_fopen("version", "wb");
	if ((f == NULL) || (fwrite("2", 1, 1, f) != 1)) {
		sr_err("Error saving version into directory: %s",
			strerror(errno));
		if (f)
			fclose(f);
		return SR_ERR;
	}
	fclose(f);

	/* init "metadata" */
	meta = g_key_file_new();

	g_key_file_set_string(meta, "global", "sigrok version",
			sr_package_version_string_get());

	devgroup = "device 1";

	logic_channels = 0;
	enabled_logic_channels = 0;
	enabled_analog_channels = 0;
	for (l = o->sdi->channels; l; l = l->next) {
		ch = l->data;

		switch (ch->type) {
		case SR_CHANNEL_LOGIC:
			if (ch->enabled)
				enabled_logic_channels++;
			logic_channels++;
			break;
		case SR_CHANNEL_ANALOG:
			if (ch->enabled)
				enabled_analog_channels++;
			break;
		}
	}

	/* When reading the file, the first index of the analog channels
	 * can only be deduced through the "total probes" count, so the
	 * first analog index must follow the last logic one, enabled or not. */
	if (enabled_logic_channels > 0)
		outc->first_analog_index = logic_channels + 1;
	else
		outc->first_analog_index = 1;

	/* Only set capturefile and probes if we will actually save logic data. */
	if (enabled_logic_channels > 0) {
		g_key_file_set_string(meta, devgroup, "capturefile", "logic-1");
		g_key_file_set_integer(meta, devgroup, "total probes", logic_channels);
	}

	s = sr_samplerate_string(outc->samplerate);
	g_key_file_set_string(meta, devgroup, "samplerate", s);
	g_free(s);

	g_key_file_set_integer(meta, devgroup, "total analog", enabled_analog_channels);

	outc->analog_ch_count = enabled_analog_channels;
	alloc_size = sizeof(gint) * outc->analog_ch_count + 1;
	outc->analog_index_map = g_malloc0(alloc_size);

	index = 0;
	for (l = o->sdi->channels; l; l = l->next) {
		ch = l->data;
		if (!ch->enabled)
			continue;

		s = NULL;
		switch (ch->type) {
		case SR_CHANNEL_LOGIC:
			ch_nr = ch->index + 1;
			s = g_strdup_printf("probe%zu", ch_nr);
			break;
		case SR_CHANNEL_ANALOG:
			ch_nr = outc->first_analog_index + index;
			outc->analog_index_map[index] = ch->index;
			s = g_strdup_printf("analog%zu", ch_nr);
			index++;
			break;
		}
		if (s) {
			g_key_file_set_string(meta, devgroup, s, ch->name);
			g_free(s);
		}
	}

	/*
	 * Allocate one samples buffer for all logic channels, and
	 * several samples buffers for the analog channels. Allocate
	 * buffers of CHUNK_SIZE size (in bytes), and determine the
	 * sample counts from the respective channel counts and data
	 * type widths.
	 *
	 * These buffers are intended to reduce the number of directory
	 * archive update calls, and decouple the srdir output module
	 * from implementation details in other acquisition device
	 * drivers and input modules.
	 *
	 * Avoid allocating zero bytes, to not depend on platform
	 * specific malloc(0) return behaviour. Avoid division by zero,
	 * holding a local buffer won't harm when no data is seen later
	 * during execution. This simplifies other locations.
	 */
	alloc_size = CHUNK_SIZE;
	outc->logic_buff.unit_size = logic_channels;
	outc->logic_buff.unit_size += 8 - 1;
	outc->logic_buff.unit_size /= 8;
	outc->logic_buff.samples = g_try_malloc0(alloc_size);
	if (!outc->logic_buff.samples)
		return SR_ERR_MALLOC;
	if (outc->logic_buff.unit_size)
		alloc_size /= outc->logic_buff.unit_size;
	outc->logic_buff.alloc_size = alloc_size;
	outc->logic_buff.fill_size = 0;
	outc->logic_buff.next_chunk_num = 1;

	alloc_size = sizeof(outc->analog_buff[0]) * outc->analog_ch_count + 1;
	outc->analog_buff = g_malloc0(alloc_size);
	for (index = 0; index < outc->analog_ch_count; index++) {
		alloc_size = CHUNK_SIZE;
		outc->analog_buff[index].samples = g_try_malloc0(alloc_size);
		if (!outc->analog_buff[index].samples)
			return SR_ERR_MALLOC;
		alloc_size /= sizeof(outc->analog_buff[0].samples[0]);
		outc->analog_buff[index].alloc_size = alloc_size;
		outc->analog_buff[index].fill_size = 0;
		outc->analog_buff[index].next_chunk_num = 1;
	}

	if (outc->logic_buff.unit_size > 0)
		g_key_file_set_integer(meta, "device 1", "unitsize",
				outc->logic_buff.unit_size);

	metabuf = g_key_file_to_data(meta, &metalen, NULL);
	g_key_file_free(meta);

	f = g_fopen("metadata", "wb");
	if ((f == NULL) || (fwrite(metabuf, metalen, 1, f) != 1)) {
		sr_err("Error saving metadata into directory: %s",
			strerror(errno));
		if (f)
			fclose(f);
		return SR_ERR;
	}
	fclose(f);
	g_free(metabuf);

	return SR_OK;
}

/**
 * Append a block of logic data to an srdir archive.
 *
 * @param[in] o Output module instance.
 * @param[in] buf Logic data samples as byte sequence.
 * @param[in] unitsize Logic data unit size (bytes per sample).
 * @param[in] length Byte sequence length (in bytes, not samples).
 *
 * @returns SR_OK et al error codes.
 */
static int dir_append(const struct sr_output *o,
	uint8_t *buf, size_t unitsize, size_t length)
{
	char *chunkname;
	struct out_context *outc;

	outc = o->priv;

	sr_dbg("%s unitsize=%ld, length=%ld", __FUNCTION__, unitsize, length);

	if (!length)
		return SR_OK;

	if (length % unitsize != 0) {
		sr_warn("Chunk size %zu not a multiple of the"
			" unit size %zu.", length, unitsize);
	}
	chunkname = g_strdup_printf("logic-1-%u",
			outc->logic_buff.next_chunk_num);
	FILE *f = g_fopen(chunkname, "wb");
	if ((f == NULL) || (fwrite(buf, length, 1, f) != 1)) {
		sr_err("Failed to add chunk '%s': %s",
			chunkname, strerror(errno));
		if (f)
			fclose(f);
		g_free(chunkname);
		return SR_ERR;
	}
	fclose(f);
	g_free(chunkname);

	outc->logic_buff.next_chunk_num++;

	return SR_OK;
}

/**
 * Queue a block of logic data for srdir archive writes.
 *
 * @param[in] o Output module instance.
 * @param[in] buf Logic data samples as byte sequence.
 * @param[in] unitsize Logic data unit size (bytes per sample).
 * @param[in] length Number of bytes of sample data.
 * @param[in] flush Force directory archive update (queue by default).
 *
 * @returns SR_OK et al error codes.
 */
static int dir_append_queue(const struct sr_output *o,
	uint8_t *buf, size_t unitsize, size_t length, gboolean flush)
{
	struct out_context *outc;
	struct logic_buff *buff;
	size_t send_size, remain, copy_size;
	uint8_t *wrptr, *rdptr;
	int ret;

	sr_dbg("%s unitsize=%ld, length=%ld", __FUNCTION__, unitsize, length);

	outc = o->priv;
	buff = &outc->logic_buff;
	if (length && unitsize != buff->unit_size) {
		sr_warn("Unexpected unit size, discarding logic data.");
		return SR_ERR_ARG;
	}

	/*
	 * Queue most recently received samples to the local buffer.
	 * Flush to the directory archive when the buffer space is exhausted.
	 */
	rdptr = buf;
	send_size = buff->unit_size ? length / buff->unit_size : 0;
	while (send_size) {
		remain = buff->alloc_size - buff->fill_size;
		if (remain) {
			wrptr = &buff->samples[buff->fill_size * buff->unit_size];
			copy_size = MIN(send_size, remain);
			send_size -= copy_size;
			buff->fill_size += copy_size;
			memcpy(wrptr, rdptr, copy_size * buff->unit_size);
			rdptr += copy_size * buff->unit_size;
			remain -= copy_size;
		}
		if (send_size && !remain) {
			ret = dir_append(o, buff->samples, buff->unit_size,
				buff->fill_size * buff->unit_size);
			if (ret != SR_OK)
				return ret;
			buff->fill_size = 0;
			remain = buff->alloc_size - buff->fill_size;
		}
	}

	/* Flush to the directory archive if the caller wants us to. */
	if (flush && buff->fill_size) {
		ret = dir_append(o, buff->samples, buff->unit_size,
			buff->fill_size * buff->unit_size);
		if (ret != SR_OK)
			return ret;
		buff->fill_size = 0;
	}

	return SR_OK;
}

/**
 * Append analog data of a channel to an srdir archive.
 *
 * @param[in] o Output module instance.
 * @param[in] values Sample data as array of floating point values.
 * @param[in] count Number of samples (float items, not bytes).
 * @param[in] ch_nr 1-based channel number.
 *
 * @returns SR_OK et al error codes.
 */
static int dir_append_analog(const struct sr_output *o,
	const float *values, size_t count, size_t ch_nr)
{
	size_t size;
	char *basename;
	char *chunkname;
	FILE *f;
	int sr_status;
	struct out_context *outc;
	struct analog_buff *buff;

	sr_dbg("%s", __FUNCTION__);

	outc = o->priv;
	buff = &outc->analog_buff[ch_nr - outc->first_analog_index];

	basename = g_strdup_printf("analog-1-%zu", ch_nr);

	size = sizeof(values[0]) * count;
	chunkname = g_strdup_printf("%s-%u", basename, buff->next_chunk_num);

	sr_status = SR_OK;
	f = g_fopen(chunkname, "wb");
	if ((f == NULL) || (fwrite(values, size, 1, f) != 1)) {
		sr_err("Failed to add chunk '%s': %s", chunkname,
			strerror(errno));
		sr_status = SR_ERR;
	}
	if (f)
		fclose(f);
	g_free(chunkname);
	g_free(basename);

	buff->next_chunk_num++;

	return sr_status;
}

/**
 * Queue analog data of a channel for srdir archive writes.
 *
 * @param[in] o Output module instance.
 * @param[in] analog Sample data (session feed packet format).
 * @param[in] flush Force directory archive update (queue by default).
 *
 * @returns SR_OK et al error codes.
 */
static int dir_append_analog_queue(const struct sr_output *o,
	const struct sr_datafeed_analog *analog, gboolean flush)
{
	struct out_context *outc;
	const struct sr_channel *ch;
	size_t idx, nr;
	struct analog_buff *buff;
	float *values, *wrptr, *rdptr;
	size_t send_size, remain, copy_size;
	int ret;

	sr_dbg("%s analog=%p, flush=%d", __FUNCTION__, analog, flush);

	outc = o->priv;

	/* Is this the DF_END flush call without samples submission? */
	if (!analog && flush) {
		for (idx = 0; idx < outc->analog_ch_count; idx++) {
			nr = outc->first_analog_index + idx;
			buff = &outc->analog_buff[idx];
			if (!buff->fill_size)
				continue;
			ret = dir_append_analog(o,
				buff->samples, buff->fill_size, nr);
			if (ret != SR_OK)
				return ret;
			buff->fill_size = 0;
		}
		return SR_OK;
	}

	/* Lookup index and number of the analog channel. */
	/* TODO: support packets covering multiple channels */
	if (g_slist_length(analog->meaning->channels) != 1) {
		sr_err("Analog packets covering multiple channels not supported yet");
		return SR_ERR;
	}
	ch = g_slist_nth_data(analog->meaning->channels, 0);
	for (idx = 0; idx < outc->analog_ch_count; idx++) {
		if (outc->analog_index_map[idx] == ch->index)
			break;
	}
	if (idx == outc->analog_ch_count)
		return SR_ERR_ARG;
	nr = outc->first_analog_index + idx;
	buff = &outc->analog_buff[idx];

	/* Convert the analog data to an array of float values. */
	values = g_try_malloc0(analog->num_samples * sizeof(values[0]));
	if (!values)
		return SR_ERR_MALLOC;
	ret = sr_analog_to_float(analog, values);
	if (ret != SR_OK) {
		g_free(values);
		return ret;
	}

	/*
	 * Queue most recently received samples to the local buffer.
	 * Flush to the directory archive when the buffer space is exhausted.
	 */
	rdptr = values;
	send_size = analog->num_samples;
	while (send_size) {
		remain = buff->alloc_size - buff->fill_size;
		if (remain) {
			wrptr = &buff->samples[buff->fill_size];
			copy_size = MIN(send_size, remain);
			send_size -= copy_size;
			buff->fill_size += copy_size;
			memcpy(wrptr, rdptr, copy_size * sizeof(values[0]));
			rdptr += copy_size;
			remain -= copy_size;
		}
		if (send_size && !remain) {
			ret = dir_append_analog(o,
				buff->samples, buff->fill_size, nr);
			if (ret != SR_OK) {
				g_free(values);
				return ret;
			}
			buff->fill_size = 0;
			remain = buff->alloc_size - buff->fill_size;
		}
	}
	g_free(values);

	/* Flush to the directory archive if the caller wants us to. */
	if (flush && buff->fill_size) {
		ret = dir_append_analog(o, buff->samples, buff->fill_size, nr);
		if (ret != SR_OK)
			return ret;
		buff->fill_size = 0;
	}

	return SR_OK;
}

static int receive(const struct sr_output *o, const struct sr_datafeed_packet *packet,
		GString **out)
{
	struct out_context *outc;
	const struct sr_datafeed_meta *meta;
	const struct sr_datafeed_logic *logic;
	const struct sr_datafeed_analog *analog;
	const struct sr_config *src;
	GSList *l;
	int ret;

	sr_dbg("%s", __FUNCTION__);

	*out = NULL;
	if (!o || !o->sdi || !(outc = o->priv))
		return SR_ERR_ARG;

	switch (packet->type) {
	case SR_DF_META:
		meta = packet->payload;
		for (l = meta->config; l; l = l->next) {
			src = l->data;
			if (src->key != SR_CONF_SAMPLERATE)
				continue;
			outc->samplerate = g_variant_get_uint64(src->data);
		}
		break;
	case SR_DF_LOGIC:
		logic = packet->payload;
		if (!outc->dir_created) {
			if ((ret = dir_create(o)) != SR_OK)
				return ret;
			outc->dir_created = TRUE;
		}
		ret = dir_append_queue(o, logic->data,
			logic->unitsize, logic->length, FALSE);
		if (ret != SR_OK)
			return ret;
		break;
	case SR_DF_ANALOG:
		/* logic channels must be stored first to have a valid unitsize */
		if (!outc->dir_created) {
			if ((ret = dir_create(o)) != SR_OK)
				return ret;
			outc->dir_created = TRUE;
		}
		analog = packet->payload;
		ret = dir_append_analog_queue(o, analog, FALSE);
		if (ret != SR_OK)
			return ret;
		break;
	case SR_DF_END:
		if (outc->dir_created) {
			ret = dir_append_queue(o, NULL, 0, 0, TRUE);
			if (ret != SR_OK)
				return ret;
			ret = dir_append_analog_queue(o, NULL, TRUE);
			if (ret != SR_OK)
				return ret;
		}
		break;
	}

	return SR_OK;
}

static struct sr_option options[] = {
	ALL_ZERO
};

static const struct sr_option *get_options(void)
{
	return options;
}

static int cleanup(struct sr_output *o)
{
	struct out_context *outc;
	size_t idx;

	outc = o->priv;

	g_free(outc->analog_index_map);
	g_free(outc->filename);
	g_free(outc->logic_buff.samples);
	for (idx = 0; idx < outc->analog_ch_count; idx++)
		g_free(outc->analog_buff[idx].samples);
	g_free(outc->analog_buff);

	g_free(outc);
	o->priv = NULL;

	return SR_OK;
}

SR_PRIV struct sr_output_module output_srdir = {
	.id = "srdir",
	.name = "srdir",
	.desc = "Session file format data stored in a directory."
			" Convert to srzip by 'cd <dir> ; zip -9 data.sr *'",
	.exts = (const char*[]){"", NULL},
	.flags = SR_OUTPUT_INTERNAL_IO_HANDLING,
	.options = get_options,
	.init = init,
	.receive = receive,
	.cleanup = cleanup,
};
