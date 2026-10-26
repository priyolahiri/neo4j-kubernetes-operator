# Excerpt of the Neo4j Docker image's entrypoint, run by the operator's tests
# of its startup-script list block (docs/knowledge/operations.md ids 179, 192,
# 193).
#
# Copied verbatim from the images neo4j:5.26.32-enterprise and
# neo4j:2026.09.0-enterprise (/startup/utilities.sh and
# /startup/docker-entrypoint.sh, identical in both; read 2026-10-25), which is
# github.com/neo4j/docker-neo4j at those release tags: from utilities.sh,
# debugging_enabled, debug_msg and containsElement; from docker-entrypoint.sh,
# check_mounted_folder_readable, load_plugin_from_location,
# apply_plugin_default_configuration, install_neo4j_plugins,
# add_docker_default_to_conf, add_env_setting_to_conf, the mounted /conf copy,
# and the "DOCKER SPECIFIC DEFAULT CONFIGURATIONS" and "ENVIRONMENT VARIABLE
# CONFIGURATIONS" passes. 5.26.0 has the same functions. The docker defaults
# are 5.26.32's: from 2025.01 the image no longer adds
# server.discovery.advertised_address, the one difference between the two
# images here. The repository's master
# has since anchored the setting match ("^[[:space:]]*<setting>[[:space:]]*=")
# where these releases match "<setting>=" as a fixed string; for a conf whose
# lines start with their key, as the operator renders it, the two agree.
# Licensed under the Apache License, Version 2.0, as that repository is.
#
# Two stand-ins, marked below, for what needs the image: is_readable (the
# image's reads ownership with GNU stat), and load_plugin_from_url, which
# downloads. It fails here as it does in the image for a plugin whose
# neo4j-plugins.json entry has no "versions" URL (fleet-management on 2026.03
# and later: the image wgets "null", says "ERROR: could not query null …" and
# starts without it), and as it would offline for any other.
#
# The caller runs install_neo4j_plugins (when NEO4J_PLUGINS is set), the docker
# defaults and then the environment pass, in that order, as the entrypoint
# does; it replaces /startup/neo4j-plugins.json with its own path. The docker
# defaults matter here for one thing: each is appended after a newline, so a
# neo4j.conf without a final newline does not run into the environment pass's
# first line.

# --- /startup/utilities.sh ---
function debugging_enabled
{
    test "${NEO4J_DEBUG+yes}" = "yes"
}

function debug_msg
{
    if debugging_enabled; then
        echo "$@"
    fi
}

function containsElement
{
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}


# --- stand-in: is_readable (the image's uses GNU stat -c and the run user) ---
function is_readable
{
    [ -r "${1}" ]
}

# --- stand-in: load_plugin_from_url (no download in a test) ---
function load_plugin_from_url
{
    echo >&2 "ERROR: could not query the versions of plugin ${1} (stand-in: no download)."
    return 1
}

# --- /startup/docker-entrypoint.sh ---
function check_mounted_folder_readable
{
    local _directory=${1}
    debug_msg "checking ${_directory} is readable"
    if ! is_readable "${_directory}"; then
        print_permissions_advice_and_fail "${_directory}" "${userid}" "${groupid}"
    fi
}

function load_plugin_from_location
{
  # Install a plugin from location at runtime.
  local _plugin_name="${1}"
  local _location="${2}"

  local _plugins_dir="${NEO4J_HOME}/plugins"
  if [ -d /plugins ]; then
    local _plugins_dir="/plugins"
  fi

  local _destination="${_plugins_dir}/${_plugin_name}.jar"

  # Now we install the plugin that is shipped with Neo4j
  for filename in ${_location}; do
    echo "Installing Plugin '${_plugin_name}' from ${_location} to ${_destination}"
    cp --preserve "${filename}" "${_destination}"
    chmod +rw ${_destination}
  done

  if ! is_readable "${_destination}"; then
    echo >&2 "Plugin at '${_destination}' is not readable"
    exit 1
  fi
}

function apply_plugin_default_configuration
{
    # Set the correct Load a plugin at runtime. The provided github repository must have a versions.json on the master branch with the
    # correct format.
    local _plugin_name="${1}" #e.g. apoc, graph-algorithms, graphql
    local _reference_conf="${2}" # used to determine if we can override properties
    local _neo4j_conf="${NEO4J_HOME}/conf/neo4j.conf"

    local _property _value
    echo "Applying default values for plugin ${_plugin_name} to neo4j.conf"
    for _entry in $(jq  --compact-output --raw-output "with_entries( select(.key==\"${_plugin_name}\") ) | to_entries[] | .value.properties | to_entries[]" /startup/neo4j-plugins.json); do
        _property="$(jq --raw-output '.key' <<< "${_entry}")"
        _value="$(jq --raw-output '.value' <<< "${_entry}")"
        debug_msg "${_plugin_name} requires setting ${_property}=${_value}"

        # the first grep strips out comments
        if grep -o "^[^#]*" "${_reference_conf}" | grep -q --fixed-strings "${_property}=" ; then
            # property is already set in the user provided config. In this case we don't override what has been set explicitly by the user.
            echo "Skipping ${_property} for plugin ${_plugin_name} because it is already set."
            echo "You may need to add ${_value} to the ${_property} setting in your configuration file."
        else
            if grep -o "^[^#]*" "${_neo4j_conf}" | grep -q --fixed-strings "${_property}=" ; then
                sed --in-place "s/${_property}=/&${_value},/" "${_neo4j_conf}"
                debug_msg "${_property} was already in the configuration file, so ${_value} was added to it."
            else
                echo -e "\n${_property}=${_value}" >> "${_neo4j_conf}"
                debug_msg "${_property}=${_value} has been added to the configuration file."
            fi
        fi
    done
}

function install_neo4j_plugins
{
    # first verify that the requested plugins are valid.
    debug_msg "One or more NEO4J_PLUGINS have been requested."
    local _known_plugins=($(jq --raw-output "keys[]" /startup/neo4j-plugins.json))
    debug_msg "Checking requested plugins are known and can be installed."
    for plugin_name in $(echo "${NEO4J_PLUGINS}" | jq --raw-output '.[]'); do
        if ! containsElement "${plugin_name}" "${_known_plugins[@]}"; then
            printf >&2 "\"%s\" is not a known Neo4j plugin. Options are:\n%s" "${plugin_name}" "$(jq --raw-output "keys[1:][]" /startup/neo4j-plugins.json)"
            exit 1
        fi
    done

    # We store a copy of the config before we modify it for the plugins to allow us to see if there are user-set values in the input config that we shouldn't override
    local _old_config="$(mktemp)"
    if [ -e "${NEO4J_HOME}"/conf/neo4j.conf ]; then
        cp "${NEO4J_HOME}"/conf/neo4j.conf "${_old_config}"
    else
        touch "${NEO4J_HOME}"/conf/neo4j.conf
        touch "${_old_config}"
    fi
    for plugin_name in $(echo "${NEO4J_PLUGINS}" | jq --raw-output '.[]'); do
        debug_msg "Plugin ${plugin_name} has been requested"
        local _location="$(jq --raw-output "with_entries( select(.key==\"${plugin_name}\") ) | to_entries[] | .value.location" /startup/neo4j-plugins.json )"
        if [ "${_location}" != "null" -a -n "$(shopt -s nullglob; echo ${_location})" ]; then
            debug_msg "$plugin_name is already in the container at ${_location}"
            load_plugin_from_location "${plugin_name}" "${_location}"
            debug_msg "Applying plugin specific configurations."
            apply_plugin_default_configuration "${plugin_name}" "${_old_config}"
        else
            debug_msg "$plugin_name must be downloaded."
            if load_plugin_from_url "${plugin_name}"; then
                debug_msg "Applying plugin specific configurations."
                apply_plugin_default_configuration "${plugin_name}" "${_old_config}"
            fi
        fi
    done
    rm "${_old_config}"
}

function add_docker_default_to_conf
{
    # docker defaults should NOT overwrite values already in the conf file
    local _setting="${1}"
    local _value="${2}"

    if [ ! -e "${NEO4J_HOME}"/conf/neo4j.conf ] || ! grep -q "^${_setting}=" "${NEO4J_HOME}"/conf/neo4j.conf
    then
        debug_msg "Appended ${_setting}=${_value} to ${NEO4J_HOME}/conf/neo4j.conf"
        echo -e "\n"${_setting}=${_value} >> "${NEO4J_HOME}"/conf/neo4j.conf
    fi
}

function add_env_setting_to_conf
{
    # settings from environment variables should overwrite values already in the conf
    local _setting=${1}
    local _value=${2}
    local _conf_file
    local _append_not_replace_configs=("server.jvm.additional")

    # different settings need to go in different files now.
    case "$(echo ${_setting} | cut -d . -f 1)" in
        apoc)
            _conf_file="${NEO4J_HOME}"/conf/apoc.conf
        ;;
        *)
            _conf_file="${NEO4J_HOME}"/conf/neo4j.conf
        ;;
    esac

    if [ -e "${_conf_file}" ] && grep -q -F "${_setting}=" "${_conf_file}"; then
        if containsElement "${_setting}" "${_append_not_replace_configs[@]}"; then
            debug_msg "${_setting} will be appended to ${_conf_file} without replacing existing settings."
        else
            # Remove any lines containing the setting already
            debug_msg "Removing existing setting for ${_setting} in ${_conf_file}"
            sed --in-place "/^${_setting}=.*/d" "${_conf_file}"
        fi
    fi
    # Then always append setting to file
    debug_msg "Appended ${_setting}=${_value} to ${_conf_file}"
    echo "${_setting}=${_value}" >> "${_conf_file}"
}

# The docker defaults, wrapped in a function to run after install_neo4j_plugins.
function apply_docker_defaults
{
## == DOCKER SPECIFIC DEFAULT CONFIGURATIONS ===
## these should not override *any* configurations set by the user

debug_msg "Setting docker specific configuration overrides"
add_docker_default_to_conf "server.memory.pagecache.size" "512M"
add_docker_default_to_conf "server.default_listen_address" "0.0.0.0"

# set enterprise only docker defaults
if [ "${NEO4J_EDITION}" == "enterprise" ];
then
    debug_msg "Setting docker specific Enterprise Edition overrides"
    add_docker_default_to_conf "server.discovery.advertised_address" "$(hostname):5000"
    add_docker_default_to_conf "server.cluster.advertised_address" "$(hostname):6000"
    add_docker_default_to_conf "server.cluster.raft.advertised_address" "$(hostname):7000"
    add_docker_default_to_conf "server.routing.advertised_address" "$(hostname):7688"
fi
}

# The environment pass, wrapped in a function to run after install_neo4j_plugins.
function apply_environment_settings
{
## == ENVIRONMENT VARIABLE CONFIGURATIONS ===
## these override BOTH defaults and any existing values in the neo4j.conf file

# these are docker control envs that have the NEO4J_ prefix but we don't want to add to the config.
not_configs=("NEO4J_ACCEPT_LICENSE_AGREEMENT" "NEO4J_AUTH" "NEO4J_AUTH_PATH" "NEO4J_DEBUG" "NEO4J_EDITION" \
             "NEO4J_HOME" "NEO4J_PLUGINS" "NEO4J_SHA256" "NEO4J_TARBALL" "NEO4J_DEPRECATION_WARNING")

debug_msg "Applying configuration settings that have been set using environment variables."
# list env variables with prefix NEO4J_ and create settings from them
for i in $( set | grep ^NEO4J_ | awk -F'=' '{print $1}' | sort -rn ); do
    if containsElement "$i" "${not_configs[@]}"; then
        continue
    fi

    # Skip env variables with suffix _FILE, these are docker secrets
    if [[ "$i" == *"_FILE" ]]; then
        continue
    fi

    setting=$(echo "${i}" | sed 's|^NEO4J_||' | sed 's|_|.|g' | sed 's|\.\.|_|g')
    value=$(echo "${!i}")
    # Don't allow settings with no value or settings that start with a number (neo4j converts settings to env variables and you cannot have an env variable that starts with a number)
    if [[ -n ${value} ]]; then
        if [[ ! "${setting}" =~ ^[0-9]+.*$ ]]; then
            add_env_setting_to_conf "${setting}" "${value}"
        else
            echo >&2 "WARNING: ${setting} not written to conf file. Settings that start with a number are not permitted."
        fi
    fi
done
}

# The mounted /conf (docker-entrypoint.sh lines 479-483 in both images, under
# "CHECK FILE PERMISSIONS ON MOUNTED FOLDERS"), wrapped in a function to run
# after the operator's startup script and before install_neo4j_plugins, as the
# entrypoint does: it empties ${NEO4J_HOME}/conf, the image's own, and copies
# the mounted directory into it. Verbatim but for the directory, the
# function's argument here (/conf in the image).
function apply_mounted_conf
{
local _mounted_conf=${1}
if [ -d "${_mounted_conf}" ]; then
    check_mounted_folder_readable "${_mounted_conf}"
    rm -rf "${NEO4J_HOME}"/conf/*
    debug_msg "Copying contents of ${_mounted_conf} to ${NEO4J_HOME}/conf/*"
    find "${_mounted_conf}" -type f -exec cp --preserve=ownership,mode {} "${NEO4J_HOME}"/conf \;
fi
}
